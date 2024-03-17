package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.*;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.Sql;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class BatchTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements(
            Sql.of(CREATE, TABLE, "t_batch (id LONG, name TEXT, age LONG)"),
            Sql.of(INSERT,  INTO, "t_batch VALUES(1, 'Sophia', 33)"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBatch(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.addBatch("UPDATE t_batch SET [name]='ccc'");
            st.addBatch("UPDATE t_batch SET age=95");
            st.executeBatch();
            checkQuery(Sql.of(SELECT, "*", FROM, "t_batch"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBatchPreparedStatement(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (PreparedStatement st = ucanaccess.prepareStatement("UPDATE t_batch SET [name]=?,age=? ")) {
            st.setString(1, "ciao");
            st.setInt(2, 23);
            st.addBatch();
            st.setString(1, "ciao1");
            st.setInt(2, 43);
            st.addBatch();
            st.executeBatch();
            checkQuery(Sql.of(SELECT, "*", FROM, "t_batch"));
        }
    }

}
