package dev.denis.hugeupload.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Application-level upload policy.
 *
 * <p>Note there are two size ceilings in this app, deliberately:
 * <ul>
 *   <li>{@code spring.webflux.multipart.max-disk-usage-per-part} — the framework's limit, enforced by
 *       {@code PartEventHttpMessageReader} while the bytes are still arriving.</li>
 *   <li>{@link #maxBytes()} — ours, enforced a layer up where we can produce a useful error body.</li>
 * </ul>
 * The framework ceiling is configured slightly higher so that ours trips first and the caller gets a
 * meaningful message instead of a generic framework rejection.
 *
 * @param storageRoot            directory uploaded bytes are written to
 * @param maxBytes               largest upload we accept
 * @param maxMetadataBytes       largest {@code metadata} form field we accept
 * @param decoderChunkSize       how much of the body Reactor Netty hands over at a time
 * @param legacyEndpointsEnabled whether to expose the demonstration anti-pattern endpoints
 */
@ConfigurationProperties("upload")
public record UploadProperties(
        @DefaultValue("${java.io.tmpdir}/huge-upload/storage") String storageRoot,
        @DefaultValue("5GB") DataSize maxBytes,
        @DefaultValue("64KB") DataSize maxMetadataBytes,
        @DefaultValue("256KB") DataSize decoderChunkSize,
        @DefaultValue("true") boolean legacyEndpointsEnabled) {
}
