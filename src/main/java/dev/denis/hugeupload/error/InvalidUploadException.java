package dev.denis.hugeupload.error;

/** The request itself is not usable: missing part, malformed metadata, wrong part order. */
public class InvalidUploadException extends RuntimeException {

    public InvalidUploadException(String message) {
        super(message);
    }

    public InvalidUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}
