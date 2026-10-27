package net.ucanaccess.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class WarningLogTest {

    @Test
    void warn_belowLimit_logsAtWarning() {
        CapturingLogger logger = new CapturingLogger(Level.INFO);
        WarningLog log = new WarningLog(logger, 2);

        log.warn("Failed to create table ''{0}''", "a");
        log.warn("Failed to create table ''{0}''", "b");
        log.summarize();

        assertThat(logger.lines).containsExactly(
            "WARNING Failed to create table 'a'",
            "WARNING Failed to create table 'b'");
    }

    @Test
    void warn_aboveLimit_logsAtDebugAndSummarizes() {
        CapturingLogger logger = new CapturingLogger(Level.INFO);
        WarningLog log = new WarningLog(logger, 2);

        for (String tn : List.of("a", "b", "c", "d")) {
            log.warn("Failed to create table ''{0}''", tn);
        }
        log.warn("Unknown expression: {0}", "x");
        log.summarize();

        assertThat(logger.lines).containsExactly(
            "WARNING Failed to create table 'a'",
            "WARNING Failed to create table 'b'",
            "DEBUG Failed to create table 'c'",
            "DEBUG Failed to create table 'd'",
            "WARNING Unknown expression: x",
            "WARNING 2 more warnings like \"Failed to create table 'a'\" were logged at level DEBUG");
    }

    @Test
    void summarize_twice_startsCountingAnew() {
        CapturingLogger logger = new CapturingLogger(Level.INFO);
        WarningLog log = new WarningLog(logger, 1);

        log.warn("Skipping table {0}", "a");
        log.warn("Skipping table {0}", "b");
        log.summarize();
        logger.lines.clear();
        log.warn("Skipping table {0}", "c");
        log.summarize();

        assertThat(logger.lines).containsExactly("WARNING Skipping table c");
    }

    @ParameterizedTest
    @CsvSource({
        "INFO,  false",
        "DEBUG, true"
    })
    void warn_withCause_attachesStackTraceOnlyAtDebug(Level enabled, boolean expectThrown) {
        CapturingLogger logger = new CapturingLogger(enabled);
        WarningLog log = new WarningLog(logger);

        log.warn(new IllegalStateException("boom"), "Cannot execute {0}: {1}", "SELECT 1", "boom");

        assertThat(logger.lines).containsExactly("WARNING Cannot execute SELECT 1: boom");
        assertThat(logger.thrown).hasSize(expectThrown ? 1 : 0);
    }

    /**
     * Records the formatted messages it receives, regardless of the enabled level.
     */
    private static final class CapturingLogger implements Logger {
        private final Level           enabled;
        private final List<String>    lines  = new ArrayList<>();
        private final List<Throwable> thrown = new ArrayList<>();

        /**
         * Creates a logger reporting levels from {@code enabled} upwards as loggable.
         */
        private CapturingLogger(Level enabled) {
            this.enabled = enabled;
        }

        /**
         * Returns the logger name.
         */
        @Override
        public String getName() {
            return "test";
        }

        /**
         * Tells whether the given level is enabled.
         */
        @Override
        public boolean isLoggable(Level level) {
            return level.getSeverity() >= enabled.getSeverity();
        }

        /**
         * Records a message with an exception.
         */
        @Override
        public void log(Level level, ResourceBundle bundle, String msg, Throwable ex) {
            lines.add(level + " " + msg);
            if (ex != null) {
                thrown.add(ex);
            }
        }

        /**
         * Records a formatted message.
         */
        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            lines.add(level + " " + (params == null || params.length == 0 ? format : MessageFormat.format(format, params)));
        }
    }

}
