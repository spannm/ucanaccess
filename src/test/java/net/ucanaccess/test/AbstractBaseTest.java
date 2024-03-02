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
        if (_testInfo.getTestMethod().isEmpty() || _testInfo.getDisplayName().startsWith(_testInfo.getTestMethod().get().getName())) {
            getLogger().log(Level.INFO, ">>>>>>>>>> TEST: {0} <<<<<<<<<<", _testInfo.getDisplayName());
        } else {
            getLogger().log(Level.INFO, ">>>>>>>>>> TEST: {0} ({1}) <<<<<<<<<<",
                _testInfo.getTestMethod().get().getName(), _testInfo.getDisplayName());
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

    protected static File copyFile(Path _source, File _target) {
        UcanaccessRuntimeException.requireNonNull(_source, "Source file required");
        UcanaccessRuntimeException.requireNonNull(_target, "Target file required");
        Try.catching(() -> Files.copy(_source, _target.toPath(), StandardCopyOption.REPLACE_EXISTING))
            .orThrow(e -> new UncheckedIOException("Failed to copy '" + _source + "' to '" + _target + "'", e));
        return _target;
    }

    protected static File copyFile(InputStream _in, File _target) {
        UcanaccessRuntimeException.requireNonNull(_in, "Input stream required");
        UcanaccessRuntimeException.requireNonNull(_target, "Target file required");
        Try.catching(() -> Files.copy(_in, _target.toPath(), StandardCopyOption.REPLACE_EXISTING))
            .orThrow(e -> new UncheckedIOException("Failed to copy to '" + _target + "'", e));
        return _target;
    }

}
