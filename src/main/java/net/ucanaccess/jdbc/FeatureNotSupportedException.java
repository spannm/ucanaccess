package net.ucanaccess.jdbc;

import net.ucanaccess.util.Logger;

public class FeatureNotSupportedException extends java.sql.SQLFeatureNotSupportedException {

    public enum NotSupportedMessage {
        NOT_SUPPORTED,
        NOT_SUPPORTED_YET

    }

    private static final long serialVersionUID = -6457220326288384415L;

    public FeatureNotSupportedException() {
        this(NotSupportedMessage.NOT_SUPPORTED);

    }

    public FeatureNotSupportedException(NotSupportedMessage message) {
        super(Logger.getMessage(message.name()));

    }

}
