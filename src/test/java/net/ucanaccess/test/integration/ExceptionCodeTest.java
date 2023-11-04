package net.ucanaccess.test.integration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.jdbc.IUcanaccessErrorCodes;
import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.hsqldb.error.ErrorCode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

class ExceptionCodeTest extends UcanaccessBaseTest {

    ExceptionCodeTest() {
        Locale.setDefault(Locale.US);
    }

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "boolean.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testVUKException(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {

            st.execute("INSERT INTO T(pk,b) VALUES('pippo', true)");

            assertThatThrownBy(() -> st.execute("INSERT INTO T(pk,b) VALUES('pippo', true)"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("integrity constraint violation: unique constraint or index violation")
                .hasFieldOrPropertyWithValue("ErrorCode", -ErrorCode.X_23505)
                .hasFieldOrPropertyWithValue("SQLState", "23505");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testGenException(AccessVersion _accessVersion) {
        UcanaccessSQLException ex = new UcanaccessSQLException(ExceptionMessages.CONCURRENT_PROCESS_ACCESS.name(), "ko", 11111);
        assertEquals(11111, ex.getErrorCode());
        assertEquals("ko", ex.getSQLState());
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testGException(AccessVersion _accessVersion) {
        assertThatThrownBy(() -> DriverManager.getConnection(UcanaccessDriver.URL_PREFIX + "kuso_yaro"))
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageContaining("given file does not exist")
            .hasFieldOrPropertyWithValue("ErrorCode", IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR)
            .hasFieldOrPropertyWithValue("SQLState", String.valueOf(IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR));
    }

}
