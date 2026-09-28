package dev.denis.hugeupload.legacy;

import dev.denis.hugeupload.config.UploadProperties;
import dev.denis.hugeupload.storage.StoredUpload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/**
 * A blocking, {@link InputStream}-flavoured storage backend — the shape Part 2 needs for the AWS SDK's
 * multipart upload.
 *
 * <p>It lives in this package because it is only used by the demonstration endpoints. The streaming
 * path does not need it: going reactive and then converting back to an {@code InputStream} means
 * putting the whole file somewhere in between, which is the thing this project exists to avoid.
 */
@Component
public class InputStreamStorage {

    private final Path root;

    public InputStreamStorage(UploadProperties properties) {
        this.root = Path.of(properties.storageRoot());
    }

    public StoredUpload store(InputStream in, String filename) throws IOException {
        Files.createDirectories(root);
        Path target = root.resolve(UUID.randomUUID().toString());
        MessageDigest digest = sha256();
        long written = 0;

        try (InputStream source = in;
             OutputStream out = Files.newOutputStream(target,
                     StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = source.read(buffer)) > 0) {
                digest.update(buffer, 0, read);
                out.write(buffer, 0, read);
                written += read;
            }
        }
        return new StoredUpload(target.getFileName().toString(), written,
                HexFormat.of().formatHex(digest.digest()));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the platform", e);
        }
    }
}
