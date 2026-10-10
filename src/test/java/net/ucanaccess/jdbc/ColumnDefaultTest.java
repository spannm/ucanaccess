package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:MethodName")
class ColumnDefaultTest extends UcanaccessBaseTest {

    static Stream<TestData> getTestData() {
        return Stream.of(
            new TestData("TINYINT", 99, 42, false),
            new TestData("SMALLINT", 4711, 43, false),
            new TestData("INTEGER", 4712, 44, false),
            new TestData("DOUBLE", 47.13, 45.01, false),
            new TestData("NUMERIC(8, 2)", 47.14, 46.01, false),
            new TestData("BOOLEAN", false, true, false),
            new TestData("VARCHAR", "def_varchar", "my_varchar", false));
        }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("getTestData")
    void testColumnDefaults(TestData testData) throws Exception {
        init(AccessVersion.getDefaultAccessVersion());

        try (UcanaccessStatement st = ucanaccess.createStatement()) {

            String dataTypeName = testData.dataType.replaceAll("[^a-zA-Z0-9]", "");
            String tblName = "tbl_" + dataTypeName;
            String colName = "col_" + dataTypeName;

            executeStatements(st,
                "CREATE TABLE " + tblName + " (id TEXT(50) PRIMARY KEY, "
                    + colName + ' ' + testData.dataType + " NULL DEFAULT " + (testData.quoteVal ? testData.defValue : "'" + testData.defValue + "'") + ")",
                "INSERT INTO " + tblName + " (id) VALUES ('[1] " + colName + " omitted')",
                "INSERT INTO " + tblName + " (id, " + colName + ") VALUES ('[2] " + colName + " explicit NULL', NULL)",
                "INSERT INTO " + tblName + " (id, " + colName + ") VALUES ('[3] " + colName + " some value', " + (testData.quoteVal ? testData.insValue : "'" + testData.insValue + "'") + ")");

            checkQuery("SELECT * FROM " + tblName + " WHERE id LIKE '[1]%'", recs(rec("[1] " + colName + " omitted", testData.defValue)));
            // Access Yes/No columns cannot hold NULL
            Object explicitNull = "BOOLEAN".equals(testData.dataType) ? Boolean.FALSE : null;
            checkQuery("SELECT * FROM " + tblName + " WHERE id LIKE '[2]%'", recs(rec("[2] " + colName + " explicit NULL", explicitNull)));
            checkQuery("SELECT * FROM " + tblName + " WHERE id LIKE '[3]%'", recs(rec("[3] " + colName + " some value", testData.insValue)));

            // the values written to the Access file must match
            checkQuery("SELECT * FROM " + tblName + " ORDER BY id");

        }
    }

    @Test
    void insert_preparedStatementWithQuotedNames_keepsExplicitNull() throws Exception {
        init(AccessVersion.getDefaultAccessVersion());
        executeStatements("CREATE TABLE [tbl defaults] ([row id] TEXT(50) PRIMARY KEY, [int col] INTEGER NULL DEFAULT 7)");

        try (PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO [tbl defaults] ([row id], [int col]) VALUES (?, ?)")) {
            ps.setString(1, "setNull");
            ps.setNull(2, Types.INTEGER);
            ps.executeUpdate();
            ps.setString(1, "setObject");
            ps.setObject(2, null);
            ps.executeUpdate();
            ps.setString(1, "value");
            ps.setInt(2, 3);
            ps.executeUpdate();
        }
        executeStatements("INSERT INTO [tbl defaults] ([row id]) VALUES ('omitted')");

        checkQuery("SELECT [row id], [int col] FROM [tbl defaults] ORDER BY [row id]",
            recs(rec("omitted", 7), rec("setNull", null), rec("setObject", null), rec("value", 3)));
        // the values written to the Access file must match
        checkQuery("SELECT * FROM [tbl defaults] ORDER BY [row id]");
    }

    @Test
    void insert_afterLoadingDatabaseFile_keepsExplicitNull() throws Exception {
        init(AccessVersion.getDefaultAccessVersion());
        executeStatements("CREATE TABLE tbl_load (id TEXT(50) PRIMARY KEY, int_col INTEGER NULL DEFAULT 7)");
        ucanaccess.close();

        // a copy is loaded from scratch, so its column defaults are read from the Access file
        File copy = copyFile(getFileAccDb().toPath(), createTempFileName("load"));
        try (UcanaccessConnection conn = buildConnection().withDbPath(copy.getAbsolutePath()).build();
             Statement st = conn.createStatement()) {
            st.executeUpdate("INSERT INTO tbl_load (id, int_col) VALUES ('explicit NULL', NULL)");
            st.executeUpdate("INSERT INTO tbl_load (id) VALUES ('omitted')");

            assertThat(readRows(st, "SELECT id, int_col FROM tbl_load ORDER BY id"))
                .containsExactly(Arrays.asList("explicit NULL", null), List.of("omitted", 7));
        }
    }

    private static List<List<Object>> readRows(Statement st, String sql) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        try (ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                rows.add(Arrays.asList(rs.getObject(1), rs.getObject(2)));
            }
        }
        return rows;
    }

    static class TestData {
        private final String  dataType;
        private final Object  defValue;
        private final Object  insValue;
        private final boolean quoteVal;

        TestData(String datatype, Object defValue,  Object insValue, boolean quoteVal) {
            dataType = datatype;
            this.defValue = defValue;
            this.insValue = insValue;
            this.quoteVal = quoteVal;
        }

        @Override
        public String toString() {
            return dataType + " (" + defValue + ", " + insValue + ")";
        }

    }

}
