package dev.denis.hugeupload;

import dev.denis.hugeupload.config.UploadProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Companion project for the article
 * <em>"Streaming Large File Uploads in Java Without Killing Your Server — Part 1"</em>.
 *
 * <p>Run it with a small heap to see the point of the whole exercise:
 * <pre>java -Xmx256m -jar target/huge-upload-0.1.0-SNAPSHOT.jar</pre>
 * and then push a file at it that is several times larger than that heap.
 */
@SpringBootApplication
@EnableConfigurationProperties(UploadProperties.class)
public class HugeUploadApplication {

    public static void main(String[] args) {
        SpringApplication.run(HugeUploadApplication.class, args);
    }
}
