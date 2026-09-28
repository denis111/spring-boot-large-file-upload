package dev.denis.hugeupload.api;

/**
 * What the caller gets back: where the upload landed and what we calculated while the bytes flowed
 * past. The digest costs nothing extra because we are already reading every byte exactly once.
 *
 * @param id       storage key of the stored object
 * @param filename the client-supplied filename, echoed back for the caller's convenience
 * @param size     bytes stored
 * @param sha256   hex SHA-256 of the stored bytes
 * @param elapsedMillis wall-clock time from first byte to committed file
 */
public record UploadResult(
        String id,
        String filename,
        long size,
        String sha256,
        long elapsedMillis) {
}
