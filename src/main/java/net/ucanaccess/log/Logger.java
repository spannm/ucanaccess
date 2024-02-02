package net.ucanaccess.log;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.logging.Level;

public final class Logger {

    private static PrintWriter          logPrintWriter;
    /** The resource bundle with logger messages. */
    private static final ResourceBundle MESSAGE_BUNDLE = ResourceBundle.getBundle(ILoggerResourceMessage.BUNDLE_NAME);

    private Logger() {
    }

    public static void dump() {
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(Logger::log);
    }

    public static void turnOffJackcessLog() {
        java.util.logging.Logger.getLogger("com.healthmarketscience.jackcess")
            .setLevel(Level.OFF);
    }

    public static PrintWriter getLogPrintWriter() {
        return logPrintWriter;
    }

    public static String getMessage(String _code, Object... _params) {
        String msg = Optional.ofNullable(_code).map(MESSAGE_BUNDLE::getString).orElse(_code);
        if (_code != null && _params != null && _params.length > 0) {
            msg = String.format(msg, _params);
        }
        return msg;
    }

    public static String getMessage(ILoggerResourceMessage _code, Object... _params) {
        return _code == null ? null : getMessage(_code.name(), _params);
    }

    /**
     * Prints the object followed by a line separator.
     * @param _obj the object to print
     */
    public static void log(Object _obj) {
        Optional.ofNullable(logPrintWriter).ifPresent(l -> {
            l.println(_obj);
            l.flush();
        });
    }

    public static void logWarning(String _warning) {
        System.err.println("WARNING: " + _warning);
    }

    public static void logWarning(ILoggerResourceMessage _code, String... _par) {
        logWarning(getMessage(_code, (Object[]) _par));
    }

    public static void setLogPrintWriter(PrintWriter _logPrintWriter) {
        logPrintWriter = _logPrintWriter;
    }
}
