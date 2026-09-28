package dev.denis.hugeupload.streaming;

import dev.denis.hugeupload.api.UploadResult;
import dev.denis.hugeupload.support.TestContent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePartEvent;
import org.springframework.http.codec.multipart.FormPartEvent;
import org.springframework.http.codec.multipart.PartEvent;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The claim this project exists to support: the upload is written as it arrives, rather than after
 * the whole body has landed.
 *
 * <p>The test is deliberately adversarial. The client sends one chunk and then <em>stops</em>,
 * refusing to send the rest until it has observed bytes on disk on the server side. A server that
 * buffers the request — {@code @RequestPart}, {@code MultipartFile}, {@code DataBufferUtils.join},
 * anything of that family — can never satisfy it, because it will not write anything until the
 * request body is complete, and the client will not complete the body until it sees a write. The
 * test fails by timing out rather than by passing for the wrong reason.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WritesWhileUploadingTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration FILE_WAIT = Duration.ofSeconds(30);
    private static final int CHUNK = 64 * 1024;

    static Path storageRoot;

    @LocalServerPort
    int port;

    private WebClient client;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws IOException {
        storageRoot = Files.createTempDirectory("huge-upload-streaming");
        registry.add("upload.storage-root", storageRoot::toString);
    }

    @BeforeEach
    void setUp() {
        client = WebClient.builder().baseUrl("http://localhost:" + port).build();
    }

    @Test
    @DisplayName("bytes are on disk before the client has finished sending")
    void writesToStorageBeforeTheClientFinishesSending() {
        long total = 4L * 1024 * 1024;
        AtomicLong bytesOnDiskMidUpload = new AtomicLong(-1);

        // Send one chunk, then wait for evidence that it reached storage, then send the rest.
        Flux<DataBuffer> gated = Flux.concat(
                TestContent.stream(CHUNK, CHUNK, 0),
                Mono.fromCallable(() -> {
                            bytesOnDiskMidUpload.set(awaitPartialFile());
                            return CHUNK;
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .thenMany(TestContent.stream(total - CHUNK, CHUNK, CHUNK)));

        Flux<PartEvent> body = Flux.<PartEvent>concat(
                FormPartEvent.create("metadata", TestContent.metadataJson("streamed.bin")),
                FilePartEvent.create("file", "streamed.bin", MediaType.APPLICATION_OCTET_STREAM, gated));

        ResponseEntity<UploadResult> response = client.post()
                .uri("/api/upload")
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(body, PartEvent.class)
                .retrieve()
                .toEntity(UploadResult.class)
                .block(TIMEOUT);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().size()).isEqualTo(total);
        assertThat(response.getBody().sha256()).isEqualTo(TestContent.sha256Hex(total, CHUNK));

        assertThat(bytesOnDiskMidUpload.get())
                .as("bytes on disk while the client was still sending (of %d total)", total)
                .isGreaterThan(0);
    }

    /** Waits until a partial file exists with bytes in it, then reports how many bytes it holds. */
    private long awaitPartialFile() {
        long deadline = System.nanoTime() + FILE_WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            try (Stream<Path> files = Files.list(storageRoot)) {
                OptionalLong largest = files
                        .filter(path -> path.getFileName().toString().endsWith(".part"))
                        .mapToLong(WritesWhileUploadingTest::sizeOrZero)
                        .max();
                if (largest.isPresent() && largest.getAsLong() > 0) {
                    return largest.getAsLong();
                }
            } catch (IOException e) {
                // The directory can be mid-creation; keep polling.
            }
            LockSupport.parkNanos(Duration.ofMillis(20).toNanos());
        }
        throw new AssertionError(
                "No partial file appeared within " + FILE_WAIT.toSeconds() + "s: "
                        + "the server is not writing bytes as they arrive");
    }

    private static long sizeOrZero(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            return 0L;
        }
    }
}
