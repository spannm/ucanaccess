package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

@Disabled
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
            checkQuery("SELECT * FROM " + tblName + " WHERE id LIKE '[2]%'", recs(rec("[2] " + colName + " explicit NULL", null)));
            checkQuery("SELECT * FROM " + tblName + " WHERE id LIKE '[3]%'", recs(rec("[3] " + colName + " some value", testData.insValue)));

        }
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
