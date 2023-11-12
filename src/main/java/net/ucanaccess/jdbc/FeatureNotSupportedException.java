package net.ucanaccess.jdbc;

import net.ucanaccess.log.ILoggerResourceMessage;
import net.ucanaccess.log.Logger;

/**
 * @deprecated Implement missing features or throw general Ucanacess exception.
 */
@Deprecated(forRemoval = true)
public class FeatureNotSupportedException extends java.sql.SQLFeatureNotSupportedException {

    public enum NotSupportedMessage implements ILoggerResourceMessage {
        NOT_SUPPORTED,
        NOT_SUPPORTED_YET
    }

    private static final long serialVersionUID = -6457220326288384415L;

    public FeatureNotSupportedException() {
        this(NotSupportedMessage.NOT_SUPPORTED);

    }

    public FeatureNotSupportedException(NotSupportedMessage _msg) {
        super(Logger.getMessage(_msg));

    }

}
