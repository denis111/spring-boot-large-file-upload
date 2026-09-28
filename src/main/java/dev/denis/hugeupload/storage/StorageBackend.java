package dev.denis.hugeupload.storage;

import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Where uploaded bytes end up.
 *
 * <p>The contract is deliberately reactive and end-to-end streaming: bytes are handed over as a
 * {@link Flux} of {@link DataBuffer} and written as they arrive. Nothing in this interface requires
 * the whole object in memory, which is what makes a 10GB upload and a 10MB upload cost the same heap.
 *
 * <p>This is also the seam Part 2 plugs into: an S3 backend implements this interface with
 * {@code AsyncRequestBody.fromPublisher(...)} and the rest of the application is unchanged.
 */
public interface StorageBackend {

    /**
     * Writes {@code content} to storage and returns what was stored.
     *
     * <p>Implementations must release every buffer they consume, and must leave no partial object
     * behind if the stream fails or the caller goes away.
     */
    Mono<StoredUpload> store(Flux<DataBuffer> content, StorageSpec spec);

    /** Removes a previously stored object; used for rollback. */
    Mono<Void> delete(String key);
}
