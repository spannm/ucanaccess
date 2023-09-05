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

    public static String getMessage(String _cod) {
        return messageBundle.getString(_cod);
    }

    public static String getMessage(String _cod, Object... _pars) {
        return String.format(messageBundle.getString(_cod), _pars);
    }

    public static void log(Object _obj) {
        if (logPrintWriter != null) {
            logPrintWriter.println(_obj);
            logPrintWriter.flush();
        }
    }

    public static void logMessage(Messages _cod) {
        log(messageBundle.getString(_cod.name()));
    }

    public static String getLogMessage(Messages _cod) {
        return messageBundle.getString(_cod.name());
    }

    public static void logWarning(String _warning) {
        System.err.println("WARNING:" + _warning);
    }

    public static void logWarning(Messages _cod) {
        logWarning(messageBundle.getString(_cod.name()));
    }

    public static void logParametricWarning(Messages _cod, String... _par) {
        logWarning(String.format(messageBundle.getString(_cod.name()), (Object[]) _par));
    }

    public static void setLogPrintWriter(PrintWriter _logPrintWriter) {
        Logger.logPrintWriter = _logPrintWriter;
    }
}
