package dev.denis.hugeupload.legacy;

import dev.denis.hugeupload.api.UploadMetadata;
import dev.denis.hugeupload.api.UploadResult;
import dev.denis.hugeupload.storage.StoredUpload;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;

/**
 * Demonstration endpoints. Every one of these is a way to lose.
 *
 * <p>They exist so the claims in the article are reproducible rather than asserted, and they are
 * disabled with {@code upload.legacy-endpoints-enabled=false}. <strong>Do not ship these.</strong>
 *
 * <p>Each endpoint isolates exactly one failure, so the compiler and the tests can hold the article
 * to account:
 * <ol>
 *   <li>{@code /request-part-flux} — the signature printed in v1 of the article. Compiles, looks
 *       right, and cannot stream: Spring resolves a {@code @RequestPart} by name out of a fully
 *       parsed body, and a bare {@code Flux<DataBuffer>} part gets handed to a decoder that joins it
 *       into one buffer bounded by {@code spring.http.codecs.max-in-memory-size}.</li>
 *   <li>{@code /block} — the same thing plus {@code .block()}, still printed in v1. It never gets as
 *       far as the memory question: the controller runs on a Netty event-loop thread, and blocking
 *       there is an error by construction.</li>
 *   <li>{@code /join} — the "pragmatic bridge" with the illegal blocking removed. It works, and
 *       that is the trap: it works right up until the file is bigger than the heap, because
 *       {@code DataBufferUtils.join} holds every byte before the first one is written.</li>
 * </ol>
 */
@RestController
@RequestMapping("/api/legacy")
@ConditionalOnProperty(name = "upload.legacy-endpoints-enabled", havingValue = "true", matchIfMissing = true)
public class LegacyUploadController {

    private final InputStreamStorage storage;

    public LegacyUploadController(InputStreamStorage storage) {
        this.storage = storage;
    }

    /** v1 of the article, verbatim: {@code @RequestPart("file") Flux<DataBuffer>}. */
    @PostMapping(value = "/request-part-flux", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<UploadResult> requestPartFlux(@RequestPart("file") Flux<DataBuffer> fileParts,
                                              @RequestPart("metadata") UploadMetadata metadata) {
        long startedAt = System.nanoTime();
        return DataBufferUtils.join(fileParts)
                .map(buffer -> buffer.asInputStream(true))
                .flatMap(inputStream -> storeBlocking(inputStream, metadata, startedAt));
    }

    /** v1 of the article, verbatim, including the {@code .block()}. */
    @PostMapping(value = "/block", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<UploadResult> block(@RequestPart("file") Flux<DataBuffer> fileParts,
                                    @RequestPart("metadata") UploadMetadata metadata) {
        long startedAt = System.nanoTime();
        // "A pragmatic bridge from reactive to imperative." It throws
        // IllegalStateException: block()/blockFirst()/blockLast() are blocking,
        // which is not supported in thread reactor-http-nio-N.
        InputStream inputStream = DataBufferUtils.join(fileParts)
                .map(buffer -> buffer.asInputStream(true)) // releaseOnClose
                .block();
        return storeBlocking(inputStream, metadata, startedAt);
    }

    /** The bridge with the illegal blocking fixed — so only the buffering remains. */
    @PostMapping(value = "/join", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<UploadResult> join(@RequestPart("file") FilePart file,
                                   @RequestPart("metadata") UploadMetadata metadata) {
        long startedAt = System.nanoTime();
        return DataBufferUtils.join(file.content())
                .map(buffer -> buffer.asInputStream(true))
                .flatMap(inputStream -> storeBlocking(inputStream, metadata, startedAt));
    }

    private Mono<UploadResult> storeBlocking(InputStream inputStream, UploadMetadata metadata, long startedAt) {
        return Mono.fromCallable(() -> {
                    StoredUpload stored = storage.store(inputStream, metadata.filename());
                    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000;
                    return new UploadResult(stored.key(), metadata.filename(), stored.size(),
                            stored.sha256(), elapsedMillis);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorMap(IOException.class, e -> new IllegalStateException("Legacy store failed", e));
    }
}
