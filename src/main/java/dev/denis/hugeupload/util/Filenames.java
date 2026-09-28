package dev.denis.hugeupload.util;

/**
 * Client-supplied filenames are data, not paths.
 *
 * <p>Nothing here is what keeps uploads inside the storage directory — that is guaranteed by never
 * building a path from client input at all (see {@code FileSystemStorageBackend}, which stores under
 * a server-generated UUID). This only trims a name to something safe to log, echo back in a JSON
 * response, or put in a {@code Content-Disposition} header.
 */
public final class Filenames {

    private static final int MAX_LENGTH = 120;

    private Filenames() {
    }

    public static String sanitize(String filename) {
        if (filename == null || filename.isBlank()) {
            return "unnamed";
        }
        String name = filename.replaceAll("[\\\\/]", "_").replaceAll("[^A-Za-z0-9._-]", "_");
        return name.length() <= MAX_LENGTH ? name : name.substring(0, MAX_LENGTH);
    }
}
