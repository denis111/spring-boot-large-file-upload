package dev.denis.hugeupload.api;

import dev.denis.hugeupload.service.PartEventUploadService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.PartEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The endpoint this whole project is about.
 *
 * <p>Note the signature: {@code @RequestBody Flux<PartEvent>}, not
 * {@code @RequestPart("file") Flux<DataBuffer>} and not {@code @RequestPart("file") FilePart}.
 * Both of those force Spring to parse the entire multipart body before your method is called — see
 * the README, and the {@code /api/legacy/**} endpoints for what each failure looks like in practice.
 */
@RestController
public class UploadController {

    private final PartEventUploadService uploads;

    public UploadController(PartEventUploadService uploads) {
        this.uploads = uploads;
    }

    @PostMapping(
            value = "/api/upload",
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<UploadResult>> upload(@RequestBody Flux<PartEvent> events) {
        return uploads.store(events)
                .map(result -> ResponseEntity.status(HttpStatus.CREATED).body(result));
    }
}
