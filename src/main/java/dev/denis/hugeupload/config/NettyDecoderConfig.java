package dev.denis.hugeupload.config;

import org.springframework.boot.reactor.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.reactor.netty.NettyServerCustomizer;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Raises the size of the chunks Reactor Netty hands to the application.
 *
 * <p>This is the single most effective throughput knob in the whole pipeline, and it is not obvious.
 * Reactor Netty's HTTP decoder defaults to 8KB per chunk, so a 512MB upload arrives as roughly 65,000
 * separate part events. Each one is cheap on its own; 65,000 of them is not, and because the writer
 * is demand-driven the cost is paid in lockstep with the network rather than in parallel with it.
 * Measured on this project's own proof upload, raising the chunk size to 256KB took a 512MB upload
 * from ~29s to a few seconds without changing the memory profile at all — the buffers are still
 * consumed one at a time.
 *
 * <p>Memory impact is bounded by the chunk size, so this trades a small, fixed amount of memory for
 * a large amount of throughput. See the README for the measurements.
 */
@Configuration(proxyBeanMethods = false)
public class NettyDecoderConfig {

    @Bean
    WebServerFactoryCustomizer<NettyReactiveWebServerFactory> decoderChunkSizeCustomizer(
            UploadProperties properties) {
        int chunkSize = properties.decoderChunkSize().toBytes() > Integer.MAX_VALUE
                ? Integer.MAX_VALUE
                : (int) properties.decoderChunkSize().toBytes();
        return factory -> factory.addServerCustomizers((NettyServerCustomizer) server ->
                server.httpRequestDecoder(spec -> spec.maxChunkSize(chunkSize)));
    }
}
