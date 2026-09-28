package dev.denis.hugeupload.api;

/**
 * The small JSON document the client sends alongside the file, as a {@code metadata} form field.
 *
 * <p>The {@code filename} here is what the <em>client claims</em> the file is called. It is never
 * used to build a path on disk — see {@link dev.denis.hugeupload.storage.FileSystemStorageBackend} —
 * so a hostile value cannot escape the storage directory.
 */
public record UploadMetadata(String filename, String contentType) {
}
