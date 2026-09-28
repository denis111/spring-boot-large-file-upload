package dev.denis.hugeupload.service;

import dev.denis.hugeupload.api.UploadMetadata;
import dev.denis.hugeupload.error.InvalidUploadException;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns the {@code metadata} form field into an {@link UploadMetadata}.
 *
 * <p>On the streaming path this field arrives as raw text, so parsing it is our job. That is the
 * price of not using {@code @RequestPart}, which would have to parse the whole multipart body first.
 */
@Component
public class UploadMetadataCodec {

    private final JsonMapper jsonMapper;

    public UploadMetadataCodec(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public UploadMetadata decode(String json) {
        if (json == null || json.isBlank()) {
            throw new InvalidUploadException("Part 'metadata' is empty; send a JSON object such as "
                    + "{\"filename\":\"video.mp4\",\"contentType\":\"video/mp4\"}");
        }
        UploadMetadata metadata;
        try {
            metadata = jsonMapper.readValue(json, UploadMetadata.class);
        } catch (JacksonException e) {
            throw new InvalidUploadException("Part 'metadata' is not valid JSON: " + e.getMessage(), e);
        }
        if (metadata.filename() == null || metadata.filename().isBlank()) {
            throw new InvalidUploadException("Part 'metadata' must contain a non-blank 'filename'");
        }
        return metadata;
    }
}
