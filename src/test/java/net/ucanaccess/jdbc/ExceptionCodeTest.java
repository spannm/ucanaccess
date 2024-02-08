package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.hsqldb.error.ErrorCode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;

class ExceptionCodeTest extends UcanaccessBaseTest {

    @BeforeAll
    static void setLocale() {
        locale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @AfterAll
    static void resetLocale() {
        Locale.setDefault(Objects.requireNonNullElseGet(locale, Locale::getDefault));
    }

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "boolean.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testVUKException(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {

            st.execute("INSERT INTO T(pk, b) VALUES('pippo', true)");

            assertThatThrownBy(() -> st.execute("INSERT INTO T(pk, b) VALUES('pippo', true)"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("integrity constraint violation: unique constraint or index violation")
                .hasFieldOrPropertyWithValue("ErrorCode", -ErrorCode.X_23505)
                .hasFieldOrPropertyWithValue("SQLState", "23505");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testGenException(AccessVersion _accessVersion) {
        UcanaccessSQLException ex = new UcanaccessSQLException("foo", "ko", 11111);
        assertEquals(11111, ex.getErrorCode());
        assertEquals("ko", ex.getSQLState());
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testGException(AccessVersion _accessVersion) {
        assertThatThrownBy(() -> buildConnection()
            .withDbPath("kuso_yaro")
            .withUser("")
            .build())
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("given file does not exist")
                .hasFieldOrPropertyWithValue("ErrorCode", IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR)
                .hasFieldOrPropertyWithValue("SQLState", String.valueOf(IUcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR));
    }

}
