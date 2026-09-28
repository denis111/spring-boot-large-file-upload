package dev.denis.hugeupload.legacy;

import dev.denis.hugeupload.api.UploadResult;
import dev.denis.hugeupload.support.CapturedLogs;
import dev.denis.hugeupload.support.TestContent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.MultiValueMap;
import org.springframework.http.HttpEntity;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the failure mode of each demonstration endpoint, so the article's claims are checked by the
 * build rather than by the reader's good faith.
 *
 * <p>If a future Spring version changes any of this behaviour, these tests fail loudly — which is
 * exactly what you want from an article that tells people not to write code this way.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                // So the assertion can read the failure reason out of the response body.
                "server.error.include-message=always",
                // Keep our own ceiling well clear of the framework's, so the framework's is what trips.
                "upload.max-bytes=100MB"
        })
class LegacyEndpointsTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int CHUNK = 64 * 1024;

    static Path storageRoot;

    @LocalServerPort
    int port;

    private WebClient client;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws IOException {
        storageRoot = Files.createTempDirectory("huge-upload-legacy");
        registry.add("upload.storage-root", storageRoot::toString);
    }

    @BeforeEach
    void setUp() {
        client = WebClient.builder().baseUrl("http://localhost:" + port).build();
    }

    @Test
    @DisplayName("@RequestPart Flux<DataBuffer> aggregates the part and hits the codec limit")
    void requestPartFluxIsCappedByTheCodecLimit() {
        // 1MB, comfortably more than the default 256KB spring.http.codecs.max-in-memory-size.
        ResponseEntity<String> response = post("/api/legacy/request-part-flux",
                TestContent.requestPartBody(TestContent.metadataJson("big.bin"),
                        TestContent.stream(1024 * 1024, CHUNK)));

        assertThat(response.getStatusCode())
                .as("a bare Flux<DataBuffer> part is joined into one buffer, bounded by max-in-memory-size")
                .isEqualTo(HttpStatus.CONTENT_TOO_LARGE);
    }

    @Test
    @DisplayName("the .block() version cannot run at all: it blocks a Netty event-loop thread")
    void blockThrowsOnTheEventLoop() {
        try (CapturedLogs logs = CapturedLogs.attach()) {
            ResponseEntity<String> response = post("/api/legacy/block",
                    TestContent.requestPartBody(TestContent.metadataJson("big.bin"),
                            TestContent.stream(256 * 1024, CHUNK)));

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
            assertThat(logs.contains("block()/blockFirst()/blockLast() are blocking"))
                    .as("the failure is structural, not a matter of tuning")
                    .isTrue();
        }
    }

    @Test
    @DisplayName("the join() bridge works - which is the trap: it holds the whole file in memory")
    void joinBridgeWorksButBuffersTheWholeFile() {
        long size = 8L * 1024 * 1024;
        ResponseEntity<UploadResult> response = postJson("/api/legacy/join",
                TestContent.requestPartBody(TestContent.metadataJson("big.bin"),
                        TestContent.stream(size, CHUNK)), UploadResult.class);

        // It succeeds. That is the point: nothing here looks wrong until the file exceeds the heap.
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().size()).isEqualTo(size);
        assertThat(response.getBody().sha256()).isEqualTo(TestContent.sha256Hex(size, CHUNK));
    }

    @Test
    @DisplayName("a JSON metadata part without a content type is rejected as 415, not 400")
    void metadataWithoutContentTypeIsUnsupportedMediaType() {
        ResponseEntity<String> response = post("/api/legacy/join",
                TestContent.requestPartBodyWithoutMetadataType(TestContent.metadataJson("big.bin"),
                        TestContent.stream(256 * 1024, CHUNK)));

        assertThat(response.getStatusCode())
                .as("binding @RequestPart to a type needs the part's content type")
                .isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
    }

    /** POSTs a multipart body and returns the response whatever its status. */
    private ResponseEntity<String> post(String uri, MultiValueMap<String, HttpEntity<?>> body) {
        ResponseEntity<String> response = client.post()
                .uri(uri)
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(body))
                .exchangeToMono(exchange -> exchange.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .map(text -> ResponseEntity.status(exchange.statusCode()).body(text)))
                .block(TIMEOUT);
        assertThat(response).isNotNull();
        return response;
    }

    private <T> ResponseEntity<T> postJson(String uri, MultiValueMap<String, HttpEntity<?>> body, Class<T> type) {
        ResponseEntity<T> response = client.post()
                .uri(uri)
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(body))
                .exchangeToMono(exchange -> exchange.bodyToMono(type)
                        .map(payload -> ResponseEntity.status(exchange.statusCode()).body(payload)))
                .block(TIMEOUT);
        assertThat(response).isNotNull();
        return response;
    }
}
