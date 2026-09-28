package dev.denis.hugeupload.storage;

/**
 * Where an upload should be stored.
 *
 * @param key         server-generated storage key. Never derived from client input.
 * @param filename    client-supplied filename, kept only as metadata about the object
 * @param contentType declared content type, kept only as metadata about the object
 */
public record StorageSpec(String key, String filename, String contentType) {
}
