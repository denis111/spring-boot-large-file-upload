package dev.denis.hugeupload.service;

import dev.denis.hugeupload.api.UploadMetadata;
import dev.denis.hugeupload.api.UploadResult;
import dev.denis.hugeupload.config.UploadProperties;
import dev.denis.hugeupload.error.InvalidUploadException;
import dev.denis.hugeupload.storage.StorageBackend;
import dev.denis.hugeupload.storage.StorageSpec;
import dev.denis.hugeupload.storage.StoredUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePartEvent;
import org.springframework.http.codec.multipart.FormPartEvent;
import org.springframework.http.codec.multipart.PartEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static dev.denis.hugeupload.util.Filenames.sanitize;

/**
 * Consumes the multipart event stream and hands the file part to storage.
 *
 * <p>Why this exists at all: {@code @RequestPart} implies map-like access to parts by name, so Spring
 * must parse the entire multipart body before your handler runs — a "file part" is then a temp file
 * on disk, the same deal as {@code MultipartFile}. {@code @RequestBody Flux<PartEvent>} is the API
 * that actually streams: each part arrives as one or more events carrying buffers as they come off
 * the wire.
 *
 * <p>Two consequences shape the code below:
 * <ul>
 *   <li>Parts are handled strictly in order, one at a time ({@code concatMap} over per-part windows),
 *       so there is nowhere for the body to pile up. The flip side is that {@code metadata} must be
 *       sent <em>before</em> {@code file} — there is no second pass to look it up.</li>
 *   <li>Every window must be consumed, even the ones we ignore, or the events queue up in memory
 *       waiting for a subscriber that never arrives.</li>
 * </ul>
 */
@Service
public class PartEventUploadService {

    private static final Logger log = LoggerFactory.getLogger(PartEventUploadService.class);

    public static final String METADATA_PART = "metadata";
    public static final String FILE_PART = "file";

    private final StorageBackend storage;
    private final UploadMetadataCodec metadataCodec;
    private final int maxMetadataBytes;

    public PartEventUploadService(StorageBackend storage,
                                  UploadMetadataCodec metadataCodec,
                                  UploadProperties properties) {
        this.storage = storage;
        this.metadataCodec = metadataCodec;
        this.maxMetadataBytes = Math.toIntExact(Math.min(properties.maxMetadataBytes().toBytes(), Integer.MAX_VALUE));
    }

    /** Stores the file part of {@code events} and returns what was stored. */
    public Mono<UploadResult> store(Flux<PartEvent> events) {
        long startedAt = System.nanoTime();
        AtomicReference<UploadMetadata> metadata = new AtomicReference<>();

        return events
                // isLast() is true on the final event of each part, which makes it the natural
                // predicate for splitting one flat event stream into one window per part.
                .windowUntil(PartEvent::isLast)
                .concatMap(window -> handlePart(window, metadata, startedAt))
                .next()
                .switchIfEmpty(Mono.error(new InvalidUploadException(
                        "No multipart part named '" + FILE_PART + "' was found in the request")));
    }

    private Flux<UploadResult> handlePart(Flux<PartEvent> window,
                                          AtomicReference<UploadMetadata> metadata,
                                          long startedAt) {
        return window.switchOnFirst((signal, partEvents) -> {
            if (!signal.hasValue()) {
                // Complete or error signal only: nothing to route.
                return partEvents.then(Mono.<UploadResult>empty());
            }
            PartEvent first = signal.get();
            return switch (first.name()) {
                case METADATA_PART -> readMetadata(partEvents, first, metadata);
                case FILE_PART -> storeFile(partEvents, first, metadata, startedAt);
                default -> drain(partEvents, first);
            };
        });
    }

    /**
     * Reads and decodes the metadata field.
     *
     * <p>A plain form field arrives as a single {@link FormPartEvent} with its value already parsed.
     * A field sent with an explicit content type — curl's {@code -F 'metadata=...;type=application/json'},
     * which is what most tutorials show — is <em>not</em> a form field as far as Spring is concerned,
     * so it arrives as ordinary part content and we join it ourselves. Bounded, because this is a
     * field we expect to be tiny and we are not willing to buffer an arbitrary part.
     */
    private Mono<UploadResult> readMetadata(Flux<PartEvent> window,
                                            PartEvent first,
                                            AtomicReference<UploadMetadata> metadata) {
        if (first instanceof FormPartEvent formPart) {
            metadata.set(metadataCodec.decode(formPart.value()));
            return window.then(Mono.<UploadResult>empty());
        }
        return DataBufferUtils.join(window.map(PartEvent::content), maxMetadataBytes)
                .doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                .map(PartEventUploadService::readUtf8AndRelease)
                .map(json -> {
                    metadata.set(metadataCodec.decode(json));
                    return json;
                })
                .then(Mono.<UploadResult>empty());
    }

    private Mono<UploadResult> storeFile(Flux<PartEvent> window,
                                         PartEvent first,
                                         AtomicReference<UploadMetadata> metadata,
                                         long startedAt) {
        UploadMetadata meta = metadata.get();
        if (meta == null) {
            // Streaming means one pass: we cannot rewind to look up a part that has not arrived yet.
            return Mono.error(new InvalidUploadException(
                    "Part '" + METADATA_PART + "' must be sent before part '" + FILE_PART + "'"));
        }
        // The metadata document is authoritative for the filename: it is the field the application
        // defined and validated. The multipart filename is transport trivia that clients mangle,
        // omit, or send as a full path, so it is only ever logged.
        String declared = meta.filename();
        if (first instanceof FilePartEvent filePart
                && filePart.filename() != null
                && !filePart.filename().equals(declared)) {
            log.debug("Multipart filename '{}' differs from metadata filename '{}'; using the metadata value",
                    filePart.filename(), declared);
        }
        StorageSpec spec = new StorageSpec(UUID.randomUUID().toString(), sanitize(declared), meta.contentType());

        return storage.store(window.map(PartEvent::content), spec)
                .map(stored -> toResult(stored, spec, startedAt));
    }

    /** Consumes and releases an unexpected part so its buffers do not sit in a queue forever. */
    private Mono<UploadResult> drain(Flux<PartEvent> window, PartEvent first) {
        log.debug("Ignoring unexpected multipart part '{}'", first.name());
        return window.map(PartEvent::content)
                .doOnNext(DataBufferUtils::release)
                .then(Mono.<UploadResult>empty());
    }

    private UploadResult toResult(StoredUpload stored, StorageSpec spec, long startedAt) {
        long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000;
        log.info("Upload complete: {} bytes in {} ms", stored.size(), elapsedMillis);
        return new UploadResult(stored.key(), spec.filename(), stored.size(), stored.sha256(), elapsedMillis);
    }

    private static String readUtf8AndRelease(DataBuffer buffer) {
        try {
            return StandardCharsets.UTF_8.decode(buffer.asByteBuffer().duplicate()).toString();
        } finally {
            DataBufferUtils.release(buffer);
        }
    }
}
