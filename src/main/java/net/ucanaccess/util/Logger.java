package net.ucanaccess.util;

import java.io.PrintWriter;
import java.util.ResourceBundle;
import java.util.logging.Level;

public final class Logger {
    public enum Messages {
        HSQLDB_DRIVER_NOT_FOUND,
        COMPLEX_TYPE_UNSUPPORTED,
        KEEP_MIRROR_AND_OTHERS,
        UNKNOWN_EXPRESSION,
        DEFAULT_VALUES_DELIMETERS,
        USER_AS_COLUMNNAME,
        ROW_COUNT,
        TRIGGER_UPDATE_CF_ERR,
        INVALID_CHARACTER_SEQUENCE,
        STATEMENT_DDL,
        CONSTRAINT,
        LOBSCALE,
        FUNCTION_ALREADY_ADDED,
        NO_SELECT
    }

    private static PrintWriter    logPrintWriter;
    private static ResourceBundle messageBundle = ResourceBundle.getBundle("net.ucanaccess.util.logger_messages");

    private Logger() {
    }

    public static void dump() {
        for (StackTraceElement el : Thread.currentThread().getStackTrace()) {
            logPrintWriter.println(el.toString());
            logPrintWriter.flush();
        }
    }

    public static void turnOffJackcessLog() {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.healthmarketscience.jackcess");
        logger.setLevel(Level.OFF);
    }

    public static PrintWriter getLogPrintWriter() {
        return logPrintWriter;
    }

    public static String getMessage(String cod) {
        return messageBundle.getString(cod);
    }

    public static String getMessage(String cod, Object... pars) {
        return String.format(messageBundle.getString(cod), pars);
    }

    public static void log(Object obj) {
        if (logPrintWriter != null) {
            logPrintWriter.println(obj);
            logPrintWriter.flush();
        }
    }

    public static void logMessage(Messages cod) {
        log(messageBundle.getString(cod.name()));
    }

    public static String getLogMessage(Messages cod) {
        return messageBundle.getString(cod.name());
    }

    public static void logWarning(String warning) {
        System.err.println("WARNING:" + warning);
    }

    public static void logWarning(Messages cod) {
        logWarning(messageBundle.getString(cod.name()));
    }

    public static void logParametricWarning(Messages cod, String... par) {
        logWarning(String.format(messageBundle.getString(cod.name()), (Object[]) par));
    }

    public static void setLogPrintWriter(PrintWriter _logPrintWriter) {
        Logger.logPrintWriter = _logPrintWriter;
    }
}
