package dev.denis.hugeupload.error;

/**
 * The upload exceeded our configured ceiling. Thrown while the bytes are still arriving, so we never
 * pay the I/O cost of receiving the rest — see {@code UploadExceptionHandler} for the HTTP mapping.
 */
public class UploadTooLargeException extends RuntimeException {

    public UploadTooLargeException(long limitBytes, long seenBytes) {
        super("Upload exceeds the configured limit of " + limitBytes + " bytes (saw " + seenBytes + " bytes)");
    }
}
