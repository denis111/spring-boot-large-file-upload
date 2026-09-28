package dev.denis.hugeupload.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Client-supplied filenames are attacker-controlled input.
 *
 * <p>The real protection is that storage keys are server-generated UUIDs, so nothing a client sends
 * can influence a path. This is the second line: what gets logged or echoed back.
 */
class FilenamesTest {

    @Test
    @DisplayName("strips path separators so a name cannot read as a traversal")
    void stripsPathSeparators() {
        assertThat(Filenames.sanitize("../../etc/passwd")).isEqualTo(".._.._etc_passwd");
        assertThat(Filenames.sanitize("..\\..\\windows\\system32\\config")).isEqualTo(".._.._windows_system32_config");
        assertThat(Filenames.sanitize("/absolute/path/file.txt")).isEqualTo("_absolute_path_file.txt");
    }

    @Test
    @DisplayName("keeps ordinary names readable")
    void keepsOrdinaryNames() {
        assertThat(Filenames.sanitize("holiday-video.mp4")).isEqualTo("holiday-video.mp4");
        assertThat(Filenames.sanitize("report_2026_final.PDF")).isEqualTo("report_2026_final.PDF");
    }

    @Test
    @DisplayName("handles blank names and caps length")
    void handlesBlankAndOverlongNames() {
        assertThat(Filenames.sanitize(null)).isEqualTo("unnamed");
        assertThat(Filenames.sanitize("   ")).isEqualTo("unnamed");
        assertThat(Filenames.sanitize("x".repeat(500))).hasSize(120);
    }

    @Test
    @DisplayName("neutralises control characters and quotes that would break a header or a log line")
    void neutralisesControlCharacters() {
        assertThat(Filenames.sanitize("evil\r\nX-Injected: 1")).isEqualTo("evil__X-Injected__1");
        assertThat(Filenames.sanitize("quote\"name.txt")).isEqualTo("quote_name.txt");
    }
}
