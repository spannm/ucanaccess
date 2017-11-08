package net.ucanaccess.test.util;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for JUnit test cases. Extends {@link Assert} in order to make all assert* methods available, while adding
 * additional custom assert methods.
 *
 * @author Markus Spann
 */
public abstract class AbstractTestBase extends Assert {

    static {
        // configure the slf4j-simple static logger binding if available
        if (isSlf4jSimpleLoggerAvailable()) {
            configureSlf4jSimpleLogger();
        }
    }

    static boolean isSlf4jSimpleLoggerAvailable() {
        try {
            Class.forName("org.slf4j.impl.SimpleLogger");
            return true;
        } catch (ClassNotFoundException _ex) {
            return false;
        }
    }

    static void configureSlf4jSimpleLogger() {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss.SSS");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
    }


    /**
     * The SLF4J logger (https://www.slf4j.org/)
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public final Logger getLogger() {
        return logger;
    }

    @Rule
    public final TestName testMethodName = new TestName();

    public String getTestMethodName() {
        return getClass().getSimpleName() + '.' + testMethodName.getMethodName() + "()";
    }

    @Before
    public void logTestBegin() {
        getLogger().info(">>>>>>>>>> BGN Test: {} >>>>>>>>>>", getTestMethodName());
    }

    @After
    public void logTestEnd() {
        getLogger().info("<<<<<<<<<< END Test: {} <<<<<<<<<<", getTestMethodName());
    }

    public static void assertEmpty(String _string) {
        Assert.assertTrue("String not empty.", _string == null || _string.length() == 0);
    }

    public static void assertNotEmpty(String _string) {
        Assert.assertTrue("String is empty.", !(_string == null || _string.length() == 0));
    }

    public static void assertContains(String _string, String _contains) {
        if (_contains != null) {
            Assert.assertTrue("String does not contain [" + _contains + "]: " + _string,
                    !(_string == null || _string.length() == 0) && _string.indexOf(_contains) >= 0);
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
