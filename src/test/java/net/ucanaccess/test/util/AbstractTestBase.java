package net.ucanaccess.test.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

/**
 * Base class for JUnit test cases. Extends {@link Assertions} in order to make all assert* methods available,
 * while adding additional custom assert methods.
 *
 * @author Markus Spann
 */
public abstract class AbstractTestBase extends Assertions {

    static {
        // configure the slf4j-simple static logger binding
        String prefix = "org.slf4j.simpleLogger.";
        System.setProperty(prefix + "logFile", "System.out");
        System.setProperty(prefix + "defaultLogLevel", "info");
        System.setProperty(prefix + "showDateTime", "true");
        System.setProperty(prefix + "dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS");
        System.setProperty(prefix + "showThreadName", "false");
    }

    /**
     * The SLF4J logger (https://www.slf4j.org/).
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /** Holds information about the current test. */
    private TestInfo     lastTestInfo;

    public final Logger getLogger() {
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

    public static void assertEmpty(String _string) {
        assertTrue("String not empty.", _string == null || _string.length() == 0);
    }

    public static void assertNotEmpty(String _string) {
        assertFalse("String is empty.", _string == null || _string.length() == 0);
    }

    public static void assertContains(String _string, String _contains) {
        if (_contains != null) {
            assertTrue("String does not contain [" + _contains + "]: " + _string,
                !(_string == null || _string.length() == 0) && _string.contains(_contains));
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

    public static final void assertTrue(String _message, boolean _expected) {
        assertTrue(_expected, _message);
    }

    public static final void assertFalse(String _message, boolean _expected) {
        assertFalse(_expected, _message);
    }

}
