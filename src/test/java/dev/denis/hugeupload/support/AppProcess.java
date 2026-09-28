package dev.denis.hugeupload.support;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Runs the packaged application as a separate process, so a test can constrain its heap.
 *
 * <p>In-process tests cannot answer the question this project is really about — "does a 512MB upload
 * fit in a 256MB heap?" — because the test and the application would share one JVM and one heap.
 *
 * <p>A single reader thread drains the process output, both to keep it available for assertions and
 * to spot the readiness line.
 */
public final class AppProcess implements AutoCloseable {

    private static final Pattern PORT_LINE = Pattern.compile("Netty started on port (\\d+)");
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(60);

    private final Process process;
    private final StringBuilder output = new StringBuilder();
    private final CountDownLatch started = new CountDownLatch(1);
    private final AtomicInteger port = new AtomicInteger(-1);

    private AppProcess(Process process) {
        this.process = process;
        Thread reader = new Thread(this::pumpOutput, "app-output-reader");
        reader.setDaemon(true);
        reader.start();
    }

    /**
     * Starts the application jar with the given heap.
     *
     * @param heap e.g. {@code "256m"}
     */
    public static AppProcess start(String heap, List<String> extraArgs) {
        Path jar = applicationJar();
        List<String> command = new ArrayList<>(List.of(
                javaExecutable().toString(),
                "-Xmx" + heap,
                "-XX:+ExitOnOutOfMemoryError",
                "-jar", jar.toString(),
                "--server.port=0"));
        command.addAll(extraArgs);
        try {
            AppProcess app = new AppProcess(new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start());
            if (!app.started.await(STARTUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                String captured = app.output();
                app.close();
                throw new IllegalStateException("Application did not start within " + STARTUP_TIMEOUT
                        + ". Output so far:\n" + captured);
            }
            return app;
        } catch (IOException e) {
            throw new IllegalStateException("Could not start the application under test", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting the application under test", e);
        }
    }

    public int port() {
        return port.get();
    }

    public String baseUrl() {
        return "http://localhost:" + port.get();
    }

    public boolean isAlive() {
        return process.isAlive();
    }

    /** Everything the process has written so far, stdout and stderr interleaved. */
    public String output() {
        synchronized (output) {
            return output.toString();
        }
    }

    public boolean outputContains(String fragment) {
        return output().contains(fragment);
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private void pumpOutput() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                synchronized (output) {
                    output.append(line).append(System.lineSeparator());
                }
                // Boot prints the bound port once the server is listening: our readiness signal.
                Matcher matcher = PORT_LINE.matcher(line);
                if (matcher.find()) {
                    port.set(Integer.parseInt(matcher.group(1)));
                    started.countDown();
                }
            }
        } catch (IOException e) {
            // The process ended; nothing more to read.
        } finally {
            // Unblock the starter if the process died before it ever listened.
            started.countDown();
        }
    }

    static Path applicationJar() {
        Path target = Path.of("target");
        try (Stream<Path> files = Files.list(target)) {
            return files
                    .filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .filter(path -> !path.getFileName().toString().endsWith("-sources.jar"))
                    .filter(path -> !path.getFileName().toString().endsWith("-javadoc.jar"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No application jar in " + target.toAbsolutePath()
                                    + " - run 'mvn package' first (failsafe does this for you)"));
        } catch (IOException e) {
            throw new IllegalStateException("Could not look for the application jar", e);
        }
    }

    static Path javaExecutable() {
        Path bin = Path.of(System.getProperty("java.home"), "bin");
        Path windows = bin.resolve("java.exe");
        return Files.exists(windows) ? windows : bin.resolve("java");
    }
}
