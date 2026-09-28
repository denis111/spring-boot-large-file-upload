package dev.denis.hugeupload.error;

/** Something went wrong on our side while persisting the upload. */
public class StorageException extends RuntimeException {

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
