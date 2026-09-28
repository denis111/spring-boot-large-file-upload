package dev.denis.hugeupload.support;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.http.codec.multipart.FilePartEvent;
import org.springframework.http.codec.multipart.FormPartEvent;
import org.springframework.http.codec.multipart.PartEvent;
import org.springframework.util.MultiValueMap;
import org.springframework.http.HttpEntity;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Generates test payloads of any size without writing them to disk or holding them in memory — the
 * same trick the application under test has to perform, applied to its own tests.
 *
 * <p>Content is deterministic: chunk {@code i} is filled from a counter, so the expected SHA-256 can
 * be computed by running the identical generator locally.
 */
public final class TestContent {

    public static final int DEFAULT_CHUNK = 64 * 1024;
    private static final DataBufferFactory FACTORY = new DefaultDataBufferFactory();

    private TestContent() {
    }

    /** The bytes of one chunk. Both the uploader and the digest calculator use this. */
    public static byte[] chunkBytes(int size, long offset) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ((offset + i) * 31);
        }
        return bytes;
    }

    /** The same content as a reactive stream, one buffer per chunk. */
    public static Flux<DataBuffer> stream(long totalBytes, int chunkSize) {
        return stream(totalBytes, chunkSize, 0);
    }

    /** Content generation starting part-way in, so a payload can be split and rejoined. */
    public static Flux<DataBuffer> stream(long totalBytes, int chunkSize, long startOffset) {
        long fullChunks = totalBytes / chunkSize;
        int remainder = (int) (totalBytes % chunkSize);
        Flux<DataBuffer> chunks = Flux.range(0, Math.toIntExact(fullChunks))
                .map(i -> FACTORY.wrap(chunkBytes(chunkSize, startOffset + i * (long) chunkSize)));
        if (remainder == 0) {
            return chunks;
        }
        return chunks.concatWith(Flux.just(
                FACTORY.wrap(chunkBytes(remainder, startOffset + fullChunks * chunkSize))));
    }

    /** SHA-256 of what {@link #stream} will produce, computed without buffering the payload. */
    public static String sha256Hex(long totalBytes, int chunkSize) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        long offset = 0;
        while (offset < totalBytes) {
            int size = (int) Math.min(chunkSize, totalBytes - offset);
            digest.update(chunkBytes(size, offset));
            offset += size;
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    public static String metadataJson(String filename) {
        return "{\"filename\":\"" + filename + "\",\"contentType\":\"application/octet-stream\"}";
    }

    /**
     * Body for the streaming endpoint: a plain-text {@code metadata} form field followed by the file
     * part. The order matters — see {@code PartEventUploadService}.
     */
    public static Flux<PartEvent> partEventBody(String metadataJson, long totalBytes, int chunkSize) {
        return Flux.<PartEvent>concat(
                FormPartEvent.create("metadata", metadataJson),
                FilePartEvent.create("file", "big.bin", MediaType.APPLICATION_OCTET_STREAM,
                        stream(totalBytes, chunkSize)));
    }

    /**
     * Body for the {@code @RequestPart} endpoints. The metadata is sent as raw JSON bytes with an
     * explicit content type, which is exactly what {@code curl -F 'metadata={...};type=application/json'}
     * puts on the wire.
     */
    public static MultiValueMap<String, HttpEntity<?>> requestPartBody(String metadataJson,
                                                                      Flux<DataBuffer> content) {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("metadata", metadataJson.getBytes(StandardCharsets.UTF_8))
                .contentType(MediaType.APPLICATION_JSON);
        builder.asyncPart("file", content, DataBuffer.class)
                .filename("big.bin")
                .contentType(MediaType.APPLICATION_OCTET_STREAM);
        return builder.build();
    }

    /** As above but with no content type on the metadata part, which is how the mistake happens. */
    public static MultiValueMap<String, HttpEntity<?>> requestPartBodyWithoutMetadataType(
            String metadataJson, Flux<DataBuffer> content) {
        MultipartBodyBuilder builder = new MultipartBodyBuilder();
        builder.part("metadata", metadataJson.getBytes(StandardCharsets.UTF_8));
        builder.asyncPart("file", content, DataBuffer.class)
                .filename("big.bin")
                .contentType(MediaType.APPLICATION_OCTET_STREAM);
        return builder.build();
    }

    /** Same as {@link #partEventBody} but with the file part first, which streaming cannot support. */
    public static Flux<PartEvent> partEventBodyFileFirst(String metadataJson, long totalBytes, int chunkSize) {
        return Flux.<PartEvent>concat(
                FilePartEvent.create("file", "big.bin", MediaType.APPLICATION_OCTET_STREAM,
                        stream(totalBytes, chunkSize)),
                FormPartEvent.create("metadata", metadataJson));
    }

    /** A body with only the file part, for testing what happens when metadata never arrives. */
    public static Flux<PartEvent> fileOnlyBody(long totalBytes, int chunkSize) {
        return FilePartEvent.create("file", "big.bin", MediaType.APPLICATION_OCTET_STREAM,
                stream(totalBytes, chunkSize)).cast(PartEvent.class);
    }

    /** A body with only the metadata field: a valid request that simply contains no file. */
    public static Flux<PartEvent> metadataOnlyBody(String metadataJson) {
        return FormPartEvent.create("metadata", metadataJson).cast(PartEvent.class).flux();
    }

    /** A body whose metadata part carries an explicit JSON content type instead of being a plain field. */
    public static Flux<PartEvent> partEventBodyWithTypedMetadata(String metadataJson,
                                                                long totalBytes,
                                                                int chunkSize) {
        byte[] json = metadataJson.getBytes(StandardCharsets.UTF_8);
        return Flux.<PartEvent>concat(
                FilePartEvent.create("metadata", "metadata.json", MediaType.APPLICATION_JSON,
                        Flux.just(FACTORY.wrap(json))),
                FilePartEvent.create("file", "big.bin", MediaType.APPLICATION_OCTET_STREAM,
                        stream(totalBytes, chunkSize)));
    }

    /** SHA-256 of a file on disk, streamed rather than read into a byte array. */
    public static String sha256OfFile(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try (InputStream in = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) > 0) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
