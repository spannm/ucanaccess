package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.*;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.Sql;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.PreparedStatement;
import java.sql.SQLException;

class BatchTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion accessVersion) throws SQLException {
        super.init(accessVersion);
        executeStatements(
            Sql.of(CREATE, TABLE, "t_batch (id LONG, name TEXT, age LONG)"),
            Sql.of(INSERT,  INTO, "t_batch VALUES(1, 'Sophia', 33)"));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBatch(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.addBatch("UPDATE t_batch SET [name]='ccc'");
            st.addBatch("UPDATE t_batch SET age=95");
            st.executeBatch();
            checkQuery(Sql.of(SELECT, "*", FROM, "t_batch"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBatchPreparedStatement(AccessVersion accessVersion) throws SQLException {
        init(accessVersion);
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

    /**
     * Batch deletes and updates find their rows through the best index of the table and fall back to a table scan
     * without a usable index. Tables without a unique index also contain duplicate rows.
     */
    @ParameterizedTest(name = "[{index}] {0} {1}")
    @CsvSource(delimiter = '|', value = {
        "id LONG PRIMARY KEY, name TEXT |                                         | false",
        "id LONG, name TEXT             | CREATE UNIQUE INDEX idx_u ON t_idx (id) | false",
        "id LONG, name TEXT             | CREATE INDEX idx_n ON t_idx (id)        | true",
        "id LONG, name TEXT             |                                         | true"
    })
    void testBatchDeleteAndUpdate(String columns, String index, boolean duplicates) throws SQLException {
        init(AccessVersion.V2010);
        executeStatements(Sql.of(CREATE, TABLE, "t_idx (", columns, ")"));
        if (index != null) {
            executeStatements(index);
        }
        try (PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_idx VALUES (?, ?)")) {
            for (int i = 1; i <= 20; i++) {
                ps.setInt(1, i);
                ps.setString(2, "row" + i);
                ps.addBatch();
                if (duplicates && i % 5 == 0) {
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
        try (PreparedStatement ps = ucanaccess.prepareStatement("DELETE FROM t_idx WHERE id = ?")) {
            for (int id : new int[] {5, 11, 12}) {
                ps.setInt(1, id);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        try (PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_idx SET name = ? WHERE id = ?")) {
            for (int id : new int[] {1, 10, 20}) {
                ps.setString(1, "upd" + id);
                ps.setInt(2, id);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        checkQuery("SELECT * FROM t_idx ORDER BY id, name");
        checkQuery("SELECT COUNT(*) FROM t_idx", singleRec(duplicates ? 20 : 17));
    }

}
