package dev.denis.hugeupload.api;

import dev.denis.hugeupload.support.TestContent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.PartEvent;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behaviour of the streaming endpoint against a real Netty server.
 *
 * <p>A real server matters: some failures in this area (blocking on an event-loop thread, for one)
 * only reproduce when the handler is genuinely running on a Netty thread.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class UploadApiTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    static Path storageRoot;
    static Path partsDirectory;

    @LocalServerPort
    int port;

    private WebClient client;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws IOException {
        storageRoot = Files.createTempDirectory("huge-upload-storage");
        partsDirectory = Files.createTempDirectory("huge-upload-parts");
        registry.add("upload.storage-root", storageRoot::toString);
        // Where a @RequestPart part would be spooled. The streaming path must never touch it.
        registry.add("spring.webflux.multipart.file-storage-directory", partsDirectory::toString);
    }

    @BeforeEach
    void setUp() {
        client = WebClient.builder().baseUrl("http://localhost:" + port).build();
    }

    @AfterEach
    void clearStorage() throws IOException {
        deleteChildren(storageRoot);
    }

    @Test
    @DisplayName("stores the upload and reports a digest that matches the bytes on disk")
    void storesUploadAndReportsDigest() throws IOException {
        long size = 8L * 1024 * 1024;
        String expectedDigest = TestContent.sha256Hex(size, TestContent.DEFAULT_CHUNK);

        ResponseEntity<UploadResult> response = client.post()
                .uri("/api/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(TestContent.partEventBody(TestContent.metadataJson("video.mp4"), size,
                        TestContent.DEFAULT_CHUNK), PartEvent.class)
                .retrieve()
                .toEntity(UploadResult.class)
                .block(TIMEOUT);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        UploadResult result = response.getBody();
        assertThat(result).isNotNull();
        assertThat(result.size()).isEqualTo(size);
        assertThat(result.filename()).isEqualTo("video.mp4");
        assertThat(result.sha256()).isEqualTo(expectedDigest);

        Path stored = storageRoot.resolve(result.id());
        assertThat(stored).exists();
        assertThat(TestContent.sha256OfFile(stored)).isEqualTo(expectedDigest);
        // committed files carry the key, not the temp suffix
        assertThat(stored.getFileName().toString()).doesNotEndWith(".part");
    }

    @Test
    @DisplayName("does not spool a streamed part to the multipart temp directory")
    void doesNotSpoolStreamedParts() throws IOException {
        client.post()
                .uri("/api/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(TestContent.partEventBody(TestContent.metadataJson("big.bin"), 4L * 1024 * 1024,
                        TestContent.DEFAULT_CHUNK), PartEvent.class)
                .retrieve()
                .toBodilessEntity()
                .block(TIMEOUT);

        assertThat(listFiles(partsDirectory))
                .as("PartEvent streaming must not spill parts to disk")
                .isEmpty();
    }

    @Test
    @DisplayName("accepts metadata sent with an explicit JSON content type")
    void acceptsTypedMetadataPart() throws IOException {
        ResponseEntity<UploadResult> response = client.post()
                .uri("/api/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(TestContent.partEventBodyWithTypedMetadata(TestContent.metadataJson("typed.bin"),
                        1024 * 1024, TestContent.DEFAULT_CHUNK), PartEvent.class)
                .retrieve()
                .toEntity(UploadResult.class)
                .block(TIMEOUT);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().filename()).isEqualTo("typed.bin");
    }

    @Test
    @DisplayName("rejects a file part that arrives before its metadata")
    void rejectsFileBeforeMetadata() {
        ResponseEntity<String> response = postExpectingFailure(TestContent.partEventBodyFileFirst(
                TestContent.metadataJson("big.bin"), 1024 * 1024, TestContent.DEFAULT_CHUNK));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("metadata");
        assertThat(listFiles(storageRoot)).isEmpty();
    }

    @Test
    @DisplayName("rejects malformed metadata with a 400 that says what to send")
    void rejectsMalformedMetadata() {
        ResponseEntity<String> response = postExpectingFailure(TestContent.partEventBody(
                "{not json", 1024 * 1024, TestContent.DEFAULT_CHUNK));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("metadata");
    }

    @Test
    @DisplayName("rejects a request with no file part")
    void rejectsRequestWithoutFilePart() {
        ResponseEntity<String> response = postExpectingFailure(
                TestContent.metadataOnlyBody(TestContent.metadataJson("nothing.bin")));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("file");
    }

    /** Posts a body and returns the response whatever its status, instead of throwing on 4xx/5xx. */
    private ResponseEntity<String> postExpectingFailure(Flux<PartEvent> body) {
        ResponseEntity<String> response = client.post()
                .uri("/api/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(body, PartEvent.class)
                .exchangeToMono(exchange -> exchange.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .map(text -> ResponseEntity.status(exchange.statusCode()).body(text)))
                .block(TIMEOUT);
        assertThat(response).isNotNull();
        return response;
    }

    private static Stream<Path> listFiles(Path directory) {
        try {
            return Files.list(directory).toList().stream();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void deleteChildren(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> children = Files.walk(directory)) {
            children.sorted(Comparator.reverseOrder())
                    .filter(path -> !path.equals(directory))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
        }
    }
}
