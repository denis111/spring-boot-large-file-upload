package dev.denis.hugeupload.storage;

/**
 * The outcome of a completed store operation.
 *
 * @param key    storage key the object was written under
 * @param size   bytes written
 * @param sha256 hex SHA-256 of the written bytes, computed during the same pass
 */
public record StoredUpload(String key, long size, String sha256) {
}
