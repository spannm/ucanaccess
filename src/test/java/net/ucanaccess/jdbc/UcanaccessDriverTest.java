package net.ucanaccess.jdbc;

import static net.ucanaccess.converters.Metadata.Property.columnOrder;
import static net.ucanaccess.converters.Metadata.Property.concatNulls;
import static net.ucanaccess.converters.Metadata.Property.encrypt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.spannm.jackcess.Database;
import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

class UcanaccessDriverTest extends UcanaccessBaseTest {

    @Test
    void testNormalizeProperties() {
        Properties input = new Properties();
        input.setProperty("columnOrder", "data");
        input.setProperty("ConcatNulls", "false"); // overwritten by url
        String url = "jdbc:ucanaccess:///tmp/testdb.mdb;CONCATNULLS=true;bo=gus;withoutVal1;withoutVal2=;enCrypt=false";
        Map<String, String> unknownProps = new LinkedHashMap<>();

        Map<Property, String> output = UcanaccessDriver.readProperties(input, url, unknownProps::put);

        assertThat(output).hasSize(3)
            .containsEntry(columnOrder, "data")
            .containsEntry(concatNulls, "true")
            .containsEntry(encrypt, "false");

        assertThat(unknownProps).hasSize(3)
            .containsEntry("bo", "gus")
            .containsEntry("withoutVal1", null)
            .containsEntry("withoutVal2", null);
    }

    @Test
    void testVersion() {
        UcanaccessDriver driver = new UcanaccessDriver();

        assertThat(driver.getMajorVersion()).isGreaterThanOrEqualTo(5);
        assertThat(driver.getMinorVersion()).isGreaterThanOrEqualTo(1);
    }

    /**
     * A class name passed via the {@code jackcessOpener}
     * property must be rejected if it does not implement {@code IJackcessOpenerInterface} -
     * and its constructor must never execute in that case, since constructor side effects
     * cannot be undone once the class has been instantiated.
     */
    @Test
    void testNewJackcessOpenerInstance_rejectsNonImplementingClass_beforeConstruction() throws Exception {
        NonOpenerWithSideEffect.instantiated = false;

        assertThatThrownBy(() -> invokeNewJackcessOpenerInstance(NonOpenerWithSideEffect.class.getName()))
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageContaining("must implement");

        assertThat(NonOpenerWithSideEffect.instantiated)
            .as("constructor of a class not implementing IJackcessOpenerInterface must never run")
            .isFalse();
    }

    @Test
    void testNewJackcessOpenerInstance_acceptsImplementingClass() throws Exception {
        Object instance = invokeNewJackcessOpenerInstance(ValidTestOpener.class.getName());

        assertThat(instance).isInstanceOf(IJackcessOpenerInterface.class);
    }

    @Test
    void testNewJackcessOpenerInstance_wrapsUnknownClassNameInSqlException() {
        assertThatThrownBy(() -> invokeNewJackcessOpenerInstance("does.not.Exist"))
            .isInstanceOf(UcanaccessSQLException.class);
    }

    private Object invokeNewJackcessOpenerInstance(String className) throws Exception {
        Method method = UcanaccessDriver.class.getDeclaredMethod("newJackcessOpenerInstance", String.class);
        method.setAccessible(true);
        try {
            return method.invoke(new UcanaccessDriver(), className);
        } catch (InvocationTargetException ex) {
            if (ex.getCause() instanceof Exception) {
                throw (Exception) ex.getCause();
            }
            throw ex;
        }
    }

    /** Simulates a malicious class unrelated to IJackcessOpenerInterface with a dangerous constructor side effect. */
    static final class NonOpenerWithSideEffect {
        static boolean instantiated = false;

        public NonOpenerWithSideEffect() {
            instantiated = true;
        }
    }

    static final class ValidTestOpener implements IJackcessOpenerInterface {
        public ValidTestOpener() {
        }

        @Override
        public Database open(File file, String password) {
            return null;
        }
    }

}
