package net.ucanaccess.test.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;

/**
 * Base class for JUnit test cases.<p>
 *
 * Logs entry and exit to/from all test methods.<br>
 * This class extends JUnit assertions to avoid the need for static imports in subclasses.
 *
 * @author Markus Spann
 */
public abstract class AbstractBaseTest extends Assertions {

    static {
        // configure the slf4j-simple static logger binding
        String prefix = "org.slf4j.simpleLogger.";
        System.setProperty(prefix + "logFile", "System.out");
        System.setProperty(prefix + "defaultLogLevel", "info");
        System.setProperty(prefix + "showShortLogName", "true");
        System.setProperty(prefix + "showDateTime", "true");
        System.setProperty(prefix + "dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS");
        System.setProperty(prefix + "showThreadName", "false");
    }

    /** The slf4j logger. */
    private Logger logger;

    /** Holds information about the current test. */
    private TestInfo lastTestInfo;

    protected final Logger getLogger() {
        if (null == logger) {
            logger = LoggerFactory.getLogger(getClass());
        }
        return logger;
    }

    @BeforeEach
    public final void setTestMethodName(TestInfo _testInfo) {
        lastTestInfo = _testInfo;
    }

    protected final String getTestMethodName() {
        if (lastTestInfo != null && lastTestInfo.getTestClass().isPresent()) {
            return lastTestInfo.getTestClass().get().getName() + '.' + lastTestInfo.getTestMethod().get().getName();
        }
        return null;
    }

    protected final String getShortTestMethodName() {
        Optional<Method> testMethod = lastTestInfo == null ? Optional.empty() : lastTestInfo.getTestMethod();
        return testMethod.map(Method::getName).orElse(null);
    }

    @BeforeEach
    public final void logTestBegin(TestInfo _testInfo) {
        logTestBeginEnd("BGN", _testInfo);
    }

    @AfterEach
    public final void logTestEnd(TestInfo _testInfo) {
        logTestBeginEnd("END", _testInfo);
    }

    protected void logTestBeginEnd(String _prefix, TestInfo _testInfo) {
        if (_testInfo.getTestMethod().isEmpty() || _testInfo.getDisplayName().startsWith(_testInfo.getTestMethod().get().getName())) {
            getLogger().info(">>>>>>>>>> {} Test: {} <<<<<<<<<<", _prefix, _testInfo.getDisplayName());
        } else {
            getLogger().info(">>>>>>>>>> {} Test: {} ({}) <<<<<<<<<<", _prefix, _testInfo.getTestMethod().get().getName(), _testInfo.getDisplayName());
        }
    }

    /**
     * Creates a subdirectory of the system's temp file directory.
     * @param _dir subdirectory name
     * @return temp directory
     * @throws UncheckedIOException If the subdirectory could not be created
     */
    protected static File createTempDir(String _dir) {
        File tempDir = new File(getTempDir());
        if (null != _dir) {
            tempDir = new File(tempDir, _dir);
        }
        if (!tempDir.exists() && !tempDir.mkdirs()) {
            throw new UncheckedIOException(new IOException("Could not create directory " + tempDir));
        }
        return tempDir;
    }

    /**
     * Returns the system's temporary directory i.e. the content of the {@code java.io.tmpdir} system property.<br>
     * Ensures the directory name ends in the system-dependent default name-separator character.
     *
     * @return system's temporary directory name
     */
    protected static String getTempDir() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        if (!tmpDir.endsWith(File.separator)) {
            tmpDir += File.separatorChar;
        }
        return tmpDir;
    }

    protected static void copyFile(Path _source, Path _target) {
        try {
            Files.copy(_source, _target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException _ex) {
            throw new UncheckedIOException("Failed to copy '" + _source + "' to '" + _target + "'", _ex);
        }
    }

    protected static void copyFile(InputStream _in, Path _target) {
        try {
            Files.copy(_in, _target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException _ex) {
            throw new UncheckedIOException("Failed to copy to '" + _target + "'", _ex);
        }
    }

    public static void assertEmpty(String _string) {
        assertTrue(_string == null || _string.isEmpty(), "String not empty");
    }

    public static void assertNotEmpty(String _string) {
        assertFalse(_string == null || _string.isEmpty(), "String is empty");
    }

    public static void assertContains(String _string, String _contains) {
        if (_contains != null) {
            assertTrue(!(_string == null || _string.length() == 0) && _string.contains(_contains), "String does not contain [" + _contains + "]: " + _string);
        }
    }

    public static void assertDoubleEquals(double _expected, double _actual) {
        assertEquals(_expected, _actual, 0.000001d);
    }

    public static void assertListEquals(List<String> _actualList, String... _expected) {
        String[] actual = new String[_actualList.size()];
        _actualList.toArray(actual);
        assertArrayEquals(_expected, actual);
    }

}
