package net.ucanaccess.log;

/**
 * Interface implemented by logger and exception messages
 * expected to exist in the resource bundle with logger messages.
 */
public interface LoggerResourceMessage {

    String BUNDLE_NAME = "net.ucanaccess.log.logger_messages";

    String name();

}
