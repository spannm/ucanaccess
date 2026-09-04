package net.ucanaccess.test;

import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.util.Try;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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

    /** The java platform/system logger. */
    private Logger   logger;

    /** Holds information about the current test. */
    private TestInfo lastTestInfo;

    protected final Logger getLogger() {
        if (null == logger) {
            logger = System.getLogger(getClass().getName());
        }
        return logger;
    }

    protected static final Logger getStaticLogger() {
        return System.getLogger(AbstractBaseTest.class.getName());
    }

    @BeforeEach
    public final void setTestMethodName(TestInfo testInfo) {
        lastTestInfo = testInfo;
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
    public final void logTestBegin(TestInfo testInfo) {
        if (testInfo.getTestMethod().isEmpty() || testInfo.getDisplayName().startsWith(testInfo.getTestMethod().get().getName())) {
            getLogger().log(Level.DEBUG, ">>>> TEST: {0} <<<<", testInfo.getDisplayName());
        } else {
            getLogger().log(Level.DEBUG, ">>>> TEST: {0} ({1}) <<<<",
                testInfo.getTestMethod().get().getName(), testInfo.getDisplayName());
        }
    }

    /**
     * Creates a subdirectory of the system's temp file directory.
     * @param dir subdirectory name
     * @return temp directory
     * @throws UncheckedIOException If the subdirectory could not be created
     */
    protected static File createTempDir(String dir) {
        File tempDir = new File(getTempDir());
        if (null != dir) {
            tempDir = new File(tempDir, dir);
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

    protected static File copyFile(Path source, File target) {
        UcanaccessRuntimeException.requireNonNull(source, "Source file required");
        UcanaccessRuntimeException.requireNonNull(target, "Target file required");
        Try.catching(() -> Files.copy(source, target.toPath(), StandardCopyOption.REPLACE_EXISTING))
            .orThrow(e -> new UncheckedIOException("Failed to copy '" + source + "' to '" + target + "'", e));
        return target;
    }

    protected static File copyFile(InputStream in, File target) {
        UcanaccessRuntimeException.requireNonNull(in, "Input stream required");
        UcanaccessRuntimeException.requireNonNull(target, "Target file required");
        Try.catching(() -> Files.copy(in, target.toPath(), StandardCopyOption.REPLACE_EXISTING))
            .orThrow(e -> new UncheckedIOException("Failed to copy to '" + target + "'", e));
        return target;
    }

}
