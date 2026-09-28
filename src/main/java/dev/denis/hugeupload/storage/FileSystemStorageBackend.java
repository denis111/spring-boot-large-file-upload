package dev.denis.hugeupload.storage;

import dev.denis.hugeupload.config.UploadProperties;
import dev.denis.hugeupload.error.StorageException;
import dev.denis.hugeupload.error.UploadTooLargeException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streams an upload straight to the filesystem.
 *
 * <p>The whole file is never held anywhere: buffers are written as they arrive, and the next one is
 * only requested once the previous write has finished. A slow disk therefore slows the client down
 * through the reactive chain instead of letting bytes pile up in memory. Memory per upload is bounded
 * by the decoder chunk size - a few hundred kilobytes - regardless of whether the upload is 5MB
 * or 50GB.
 *
 * <p><strong>Why each chunk is copied to the heap before it is written.</strong> The obvious version
 * of this class hands the incoming buffers straight to {@link AsynchronousFileChannel}. In WebFlux
 * those buffers are pooled <em>direct</em> buffers owned by Netty, and on Windows with a current JDK
 * the channel cannot write them: it needs a stable address for the buffer, and a buffer backed by a
 * shared memory session does not have one. The write fails deep inside the JDK with
 * {@code UnsupportedOperationException: ByteBuffer derived from closeable shared sessions not supported}.
 * Copying each chunk onto the heap first costs one memcpy per chunk and works everywhere. The
 * alternative - a blocking file stream on a bounded elastic thread - also works, but pays a thread
 * handoff per chunk and measured roughly twice as slow.
 *
 * <p>Two more details here are easy to get wrong:
 * <ul>
 *   <li>{@code DataBufferUtils.write} does <em>not</em> release the buffers it writes - it re-emits
 *       them. Whoever subscribes has to release them, or pooled off-heap memory leaks.</li>
 *   <li>Bytes already written stay on disk when the stream fails, so the partial file has to be
 *       deleted explicitly, including when the client simply goes away.</li>
 * </ul>
 */
@Component
public class FileSystemStorageBackend implements StorageBackend {

    private static final Logger log = LoggerFactory.getLogger(FileSystemStorageBackend.class);
    private static final String PART_SUFFIX = ".part";
    private static final DefaultDataBufferFactory HEAP_BUFFERS = new DefaultDataBufferFactory();

    private final Path root;
    private final long maxBytes;
    /**
     * Where write completions are dispatched. Left to itself the JDK uses a JVM-wide pool, which is
     * fine but invisible; a small named pool makes the concurrency explicit and keeps the threads
     * out of the way of everything else.
     */
    private final ExecutorService fileCompletionExecutor;

    public FileSystemStorageBackend(UploadProperties properties) {
        this.root = Path.of(properties.storageRoot());
        this.maxBytes = properties.maxBytes().toBytes();
        this.fileCompletionExecutor = Executors.newFixedThreadPool(4, runnable -> {
            Thread thread = new Thread(runnable, "upload-file-io");
            thread.setDaemon(true);
            return thread;
        });
    }

    @PreDestroy
    void shutdown() {
        fileCompletionExecutor.shutdown();
    }

    @Override
    public Mono<StoredUpload> store(Flux<DataBuffer> content, StorageSpec spec) {
        // The client's filename deliberately plays no part in this path: a hostile
        // "../../etc/passwd" cannot influence where anything is written.
        String key = spec.key() == null || spec.key().isBlank() ? UUID.randomUUID().toString() : spec.key();
        Path tempFile = root.resolve(key + PART_SUFFIX);
        Path targetFile = root.resolve(key);

        AtomicLong written = new AtomicLong();
        MessageDigest digest = sha256();

        Flux<DataBuffer> metered = content
                .doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                .doOnNext(buffer -> {
                    long total = written.addAndGet(buffer.readableByteCount());
                    if (total > maxBytes) {
                        // We are aborting; this buffer never reaches the writer, so releasing it is
                        // our job. Without this line the pooled buffer is simply dropped.
                        DataBufferUtils.release(buffer);
                        throw new UploadTooLargeException(maxBytes, total);
                    }
                    // Hash a duplicate: digest.update() consumes the ByteBuffer's position, and the
                    // writer downstream still needs to see the buffer's full readable range.
                    digest.update(buffer.asByteBuffer().duplicate());
                })
                // Copying to the heap doubles as the fix for writing pooled direct buffers, which the
                // file channel cannot do on every platform (see the class comment).
                .map(FileSystemStorageBackend::toHeapBuffer);

        return Mono.usingWhen(
                openChannel(tempFile),
                channel -> DataBufferUtils.write(metered, channel)
                        // write() re-emits each buffer once it is on disk: release and move on.
                        .doOnNext(DataBufferUtils::release)
                        .doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                        .then(Mono.fromCallable(
                                () -> commit(channel, tempFile, targetFile, digest, written))),
                FileSystemStorageBackend::closeQuietly,
                (channel, cause) -> closeQuietly(channel)
                        .then(deleteQuietly(tempFile))
                        .then(Mono.error(cause)),
                channel -> closeQuietly(channel).then(deleteQuietly(tempFile)));
    }

    /**
     * Copies one chunk onto the heap and releases the original.
     *
     * <p>Deliberately one buffer at a time. An earlier version gathered several chunks per write with
     * {@code bufferTimeout}, which reduced syscalls but broke correctness: the timeout emits
     * independently of downstream demand, and the file writer only ever requests one write at a time,
     * so a timer that fired between writes produced
     * {@code OverflowException: Could not emit buffer due to lack of requests} under load. Chunk size is
     * the knob that actually matters here, and it is set on the server in {@code NettyDecoderConfig}.
     */
    private static DataBuffer toHeapBuffer(DataBuffer buffer) {
        try {
            byte[] bytes = new byte[buffer.readableByteCount()];
            buffer.read(bytes);
            return HEAP_BUFFERS.wrap(bytes);
        } finally {
            DataBufferUtils.release(buffer);
        }
    }

    @Override
    public Mono<Void> delete(String key) {
        return Mono.<Void>fromRunnable(() -> {
                    try {
                        Files.deleteIfExists(root.resolve(key));
                    } catch (IOException e) {
                        throw new StorageException("Could not delete stored object " + key, e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<AsynchronousFileChannel> openChannel(Path tempFile) {
        return Mono.fromCallable(() -> {
                    Files.createDirectories(root);
                    return AsynchronousFileChannel.open(tempFile,
                            EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                            this.fileCompletionExecutor);
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Closes the channel and publishes the finished file under its final name.
     *
     * <p>The {@code .part} suffix plus an atomic move means a half-written upload is never visible
     * under the key a caller was given — the same property S3 multipart gets from completing an
     * upload rather than writing bytes to the destination directly.
     *
     * <p>Order matters on Windows: you cannot move a file that is still open.
     */
    private StoredUpload commit(AsynchronousFileChannel channel,
                                Path tempFile,
                                Path targetFile,
                                MessageDigest digest,
                                AtomicLong written) throws IOException {
        channel.close();
        try {
            Files.move(tempFile, targetFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            // Some filesystems cannot promise atomicity; a same-directory rename is still the best
            // available option.
            log.debug("Atomic move unsupported, falling back to a plain move", e);
            Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
        }
        StoredUpload stored = new StoredUpload(targetFile.getFileName().toString(),
                written.get(), HexFormat.of().formatHex(digest.digest()));
        log.info("Stored {} ({} bytes, sha256={})", stored.key(), stored.size(), stored.sha256());
        return stored;
    }

    private static Mono<Void> closeQuietly(AsynchronousFileChannel channel) {
        return Mono.fromRunnable(() -> {
            try {
                channel.close();
            } catch (Exception e) {
                log.debug("Could not close upload channel", e);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private static Mono<Void> deleteQuietly(Path path) {
        return Mono.fromRunnable(() -> {
            try {
                Files.deleteIfExists(path);
            } catch (Exception e) {
                log.warn("Could not remove partial upload {}", path, e);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the platform", e);
        }
    }
}
