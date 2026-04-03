package net.ucanaccess.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Utility for initializing {@code java.util.logging} (JUL) from properties.
 */
public final class LoggingConfigurator {

    private LoggingConfigurator() {
        throw new UnsupportedOperationException("Utility class " + getClass().getSimpleName() + " cannot be instantiated");
    }

    /**
     * Loads the logging configuration from the classpath.
     * @param resourceName the name of the properties file
     */
    public static void setup(String resourceName) {
        try (InputStream is = LoggingConfigurator.class.getClassLoader().getResourceAsStream(resourceName)) {

            if (is != null) {
                LogManager.getLogManager().readConfiguration(is);
                // Log message starts with uppercase, concise style, no period
                System.out.println("Logging initialized from " + resourceName);
            } else {
                System.err.println("Logging configuration not found in classpath: " + resourceName);
            }
        } catch (IOException _ex) {
            System.err.println("Failed to load logging configuration: " + _ex.getMessage());
        }
    }

    public static void setup() {
        setup("logging.properties");
    }

    public static Logger configure(String loggerName) {
        setup();
        return Logger.getLogger(loggerName);
    }

}
