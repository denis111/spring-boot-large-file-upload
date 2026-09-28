package dev.denis.hugeupload.api;

import dev.denis.hugeupload.error.InvalidUploadException;
import dev.denis.hugeupload.error.StorageException;
import dev.denis.hugeupload.error.UploadTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ContentTooLargeException;

/**
 * Maps upload failures onto status codes a client can act on.
 *
 * <p>Errors raised inside the reactive chain (a size limit tripping mid-stream, for instance) surface
 * here too, because the controller returns that chain as its result.
 */
@RestControllerAdvice
public class UploadExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(UploadExceptionHandler.class);

    @ExceptionHandler(UploadTooLargeException.class)
    public ResponseEntity<ProblemDetail> onTooLarge(UploadTooLargeException ex) {
        return problem(HttpStatus.CONTENT_TOO_LARGE, "Upload too large", ex.getMessage());
    }

    /** Raised by {@code DataBufferUtils.join} when a part we chose to buffer exceeds its bound. */
    @ExceptionHandler(DataBufferLimitException.class)
    public ResponseEntity<ProblemDetail> onBufferLimit(DataBufferLimitException ex) {
        return problem(HttpStatus.CONTENT_TOO_LARGE, "Part too large", ex.getMessage());
    }

    /** Raised by the framework's own multipart ceiling (spring.webflux.multipart.max-disk-usage-per-part). */
    @ExceptionHandler(ContentTooLargeException.class)
    public ResponseEntity<ProblemDetail> onContentTooLarge(ContentTooLargeException ex) {
        return problem(HttpStatus.CONTENT_TOO_LARGE, "Upload too large", ex.getMessage());
    }

    @ExceptionHandler(InvalidUploadException.class)
    public ResponseEntity<ProblemDetail> onInvalid(InvalidUploadException ex) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid upload request", ex.getMessage());
    }

    @ExceptionHandler(StorageException.class)
    public ResponseEntity<ProblemDetail> onStorageFailure(StorageException ex) {
        log.error("Storage failure", ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Storage failure", ex.getMessage());
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String title, String detail) {
        ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
        body.setTitle(title);
        return ResponseEntity.status(status).body(body);
    }
}
