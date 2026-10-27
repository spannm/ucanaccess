package net.ucanaccess.util;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.text.MessageFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Logs warnings without flooding the log when the same kind of warning comes up over and over.
 * <p>
 * Only the first few warnings of each kind are logged at level {@code WARNING}, further ones go to level
 * {@code DEBUG}. The kind of a warning is its message pattern. {@link #summarize()} reports how many warnings were
 * moved to {@code DEBUG}, typically at the end of a longer operation.
 * <p>
 * An example is loading a damaged database, which can cause the same warning for many tables, columns or rows.
 * <p>
 * Instances are not thread-safe.
 *
 * @since 5.2.1
 */
public final class WarningLog {
    private static final int DEFAULT_LIMIT = 5;

    private final Logger            logger;
    private final int               limit;
    private final Map<String, Kind> kinds = new LinkedHashMap<>();

    /**
     * Creates a warning log with the default limit.
     *
     * @param logger the logger to write to
     */
    public WarningLog(Logger logger) {
        this(logger, DEFAULT_LIMIT);
    }

    /**
     * Creates a warning log.
     *
     * @param logger the logger to write to
     * @param limit  the number of warnings of each kind logged at level {@code WARNING}
     */
    public WarningLog(Logger logger, int limit) {
        this.logger = logger;
        this.limit = limit;
    }

    /**
     * Logs a warning.
     * <p>
     * The pattern must be a constant, the variable parts belong into {@code params}. A pattern assembled at
     * runtime would make every warning a kind of its own.
     *
     * @param pattern the message pattern in {@link MessageFormat} syntax, identifies the kind of warning
     * @param params  the message parameters
     */
    public void warn(String pattern, Object... params) {
        warn(null, pattern, params);
    }

    /**
     * Logs a warning caused by an exception. The stack trace of the exception is only logged if level
     * {@code DEBUG} is enabled. As with {@link #warn(String, Object...)}, the pattern must be a constant.
     *
     * @param cause   the exception, may be {@code null}
     * @param pattern the message pattern in {@link MessageFormat} syntax, identifies the kind of warning
     * @param params  the message parameters
     */
    public void warn(Throwable cause, String pattern, Object... params) {
        String message = MessageFormat.format(pattern, params);
        Kind kind = kinds.computeIfAbsent(pattern, p -> new Kind(message));
        kind.count++;
        Level level = kind.count <= limit ? Level.WARNING : Level.DEBUG;
        if (cause != null && logger.isLoggable(Level.DEBUG)) {
            logger.log(level, message, cause);
        } else {
            logger.log(level, message);
        }
    }

    /**
     * Logs one warning for each kind that exceeded the limit, then starts counting anew.
     */
    public void summarize() {
        kinds.values().stream()
            .filter(k -> k.count > limit)
            .forEach(k -> logger.log(Level.WARNING, MessageFormat.format(
                "{0} more warnings like \"{1}\" were logged at level DEBUG", k.count - limit, k.first)));
        kinds.clear();
    }

    /**
     * Counts the warnings of one kind and keeps the first message for the summary.
     */
    private static final class Kind {
        private final String first;
        private int          count;

        /**
         * Creates a kind of warning.
         *
         * @param first the first message of this kind
         */
        private Kind(String first) {
            this.first = first;
        }
    }

}
