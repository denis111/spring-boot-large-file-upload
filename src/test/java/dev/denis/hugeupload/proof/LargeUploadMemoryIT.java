package dev.denis.hugeupload.proof;

import dev.denis.hugeupload.api.UploadResult;
import dev.denis.hugeupload.support.AppProcess;
import dev.denis.hugeupload.support.TestContent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.PartEvent;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The headline result, measured rather than asserted: a file several times larger than the heap.
 *
 * <p>The application runs as a separate process with a hard {@code -Xmx}, because the point of the
 * exercise is what happens to <em>its</em> heap. Each test uploads a payload generated on the fly,
 * so nothing on the test side needs the space either.
 *
 * <p>Opt-in, since it moves half a gigabyte:
 * <pre>./mvnw verify -DskipITs=false
 * ./mvnw verify -DskipITs=false -Dupload.proof.size=2GB</pre>
 */
class LargeUploadMemoryIT {

    /** Small enough to run by default, large enough that buffering it is fatal at the given heap. */
    private static final long DEFAULT_SIZE = 512L * 1024 * 1024;
    private static final String HEAP = "256m";
    private static final int CHUNK = 64 * 1024;
    private static final Duration UPLOAD_TIMEOUT = Duration.ofMinutes(10);
    private static final List<String> JVM_ARGS = List.of(
            "-XX:MaxDirectMemorySize=" + HEAP,
            "-Dio.netty.leakDetection.level=paranoid");

    private static long payloadSize() {
        String configured = System.getProperty("upload.proof.size", "");
        if (configured.isBlank()) {
            return DEFAULT_SIZE;
        }
        String value = configured.trim().toUpperCase();
        long multiplier = 1;
        if (value.endsWith("GB")) {
            multiplier = 1024L * 1024 * 1024;
            value = value.substring(0, value.length() - 2);
        } else if (value.endsWith("MB")) {
            multiplier = 1024L * 1024;
            value = value.substring(0, value.length() - 2);
        }
        return Long.parseLong(value.trim()) * multiplier;
    }

    @Test
    @DisplayName("streams a payload larger than the heap to disk, byte for byte")
    void streamsPayloadLargerThanTheHeap() {
        long size = payloadSize();
        String expectedDigest = TestContent.sha256Hex(size, CHUNK);

        try (AppProcess app = AppProcess.start(HEAP, JVM_ARGS)) {
            ResponseEntity<UploadResult> response = streamingUpload(app, TestContent.partEventBody(
                            TestContent.metadataJson("huge.bin"), size, CHUNK))
                    .block(UPLOAD_TIMEOUT);

            assertThat(response).isNotNull();
            assertThat(response.getStatusCode().is2xxSuccessful())
                    .as("a %d byte upload through a %s heap", size, HEAP)
                    .isTrue();
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().size()).isEqualTo(size);
            assertThat(response.getBody().sha256())
                    .as("every byte survived the trip")
                    .isEqualTo(expectedDigest);

            assertThat(app.isAlive()).as("the app should not have died").isTrue();
            assertThat(app.outputContains("OutOfMemoryError")).isFalse();
            // 8192 pooled buffers pass through this upload, so a leak would be fatal long before
            // this point; paranoid detection turns any that occur into a loud log line.
            assertThat(app.outputContains("LEAK:")).as("no pooled buffer should leak").isFalse();
        }
    }

    @Test
    @DisplayName("the join() bridge dies on the same payload at the same heap")
    void bridgeCannotSurviveTheSamePayload() {
        long size = payloadSize();

        try (AppProcess app = AppProcess.start(HEAP, JVM_ARGS)) {
            ResponseEntity<String> response = client(app).post()
                    .uri("/api/legacy/join")
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(BodyInserters.fromMultipartData(TestContent.requestPartBody(
                            TestContent.metadataJson("huge.bin"), TestContent.stream(size, CHUNK))))
                    .exchangeToMono(exchange -> exchange.bodyToMono(String.class)
                            .defaultIfEmpty("")
                            .map(text -> ResponseEntity.status(exchange.statusCode()).body(text)))
                    .onErrorResume(error -> Mono.just(ResponseEntity.status(599).body(error.getMessage())))
                    .block(UPLOAD_TIMEOUT);

            assertThat(response == null || !response.getStatusCode().is2xxSuccessful())
                    .as("join() holds the whole payload in memory, so it cannot survive a %s heap", HEAP)
                    .isTrue();
            assertThat(app.outputContains("OutOfMemoryError"))
                    .as("the failure should be an out-of-memory, not something else")
                    .isTrue();
        }
    }

    private static Mono<ResponseEntity<UploadResult>> streamingUpload(AppProcess app, Flux<PartEvent> body) {
        return client(app).post()
                .uri("/api/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(body, PartEvent.class)
                .retrieve()
                .toEntity(UploadResult.class);
    }

    private static WebClient client(AppProcess app) {
        return WebClient.builder().baseUrl(app.baseUrl()).build();
    }
}
