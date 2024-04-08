package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.CREATE;
import static net.ucanaccess.type.SqlConstants.TABLE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.Sql;
import org.hsqldb.error.ErrorCode;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;

class ExceptionCodeTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements(Sql.of(CREATE, TABLE, "t (pk VARCHAR(10) PRIMARY KEY)"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testVUKException(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {

            String sql = "INSERT INTO t(pk) VALUES('apk')";
            st.execute(sql);

            assertThatThrownBy(() -> st.execute(sql))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("integrity constraint violation: unique constraint or index violation")
                .hasFieldOrPropertyWithValue("ErrorCode", -ErrorCode.X_23505)
                .hasFieldOrPropertyWithValue("SQLState", "23505");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testGenException(AccessVersion _accessVersion) {
        UcanaccessSQLException ex = new UcanaccessSQLException("foo", "ko", 11111);
        assertEquals(11111, ex.getErrorCode());
        assertEquals("ko", ex.getSQLState());
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
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
