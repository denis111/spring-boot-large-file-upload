package dev.denis.hugeupload.support;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Captures server-side log output for assertions.
 *
 * <p>Needed because some failures are only visible in the log: a Netty buffer leak, or an exception
 * whose message the HTTP layer deliberately withholds from the response body.
 */
public final class CapturedLogs implements AutoCloseable {

    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    private CapturedLogs() {
    }

    public static CapturedLogs attach() {
        CapturedLogs captured = new CapturedLogs();
        captured.appender.start();
        captured.root.addAppender(captured.appender);
        return captured;
    }

    public boolean contains(String fragment) {
        return text().contains(fragment);
    }

    /** All captured output, with stack traces flattened in. */
    public String text() {
        return appender.list.stream()
                .map(event -> {
                    String message = event.getFormattedMessage();
                    if (event.getThrowableProxy() != null) {
                        message += System.lineSeparator() + ThrowableProxyUtil.asString(event.getThrowableProxy());
                    }
                    return message;
                })
                .collect(Collectors.joining(System.lineSeparator()));
    }

    @Override
    public void close() {
        root.detachAppender(appender);
        appender.stop();
    }
}
