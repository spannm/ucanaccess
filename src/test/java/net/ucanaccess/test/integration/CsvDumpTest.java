package net.ucanaccess.test.integration;

import net.ucanaccess.console.Exporter;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Integration test for {@link net.ucanaccess.console.Exporter}.
 */
class CsvDumpTest extends UcanaccessBaseTest {

    // Support both Linux and Windows.
    private static final String LINE_SEPARATOR  = System.lineSeparator();

    private static final String FORMAT          = "{\"name\": \"%s\", \"type\": \"%s\", \"mode\": \"%s\"}";

    private static final String EXPECTED_SCHEMA = "[" + LINE_SEPARATOR
        + String.format(FORMAT, "id", "int64", "nullable") + "," + LINE_SEPARATOR
        + String.format(FORMAT, "text_field", "string", "nullable") + "," + LINE_SEPARATOR
        + String.format(FORMAT, "text_field2", "string", "nullable") + "," + LINE_SEPARATOR
        + String.format(FORMAT, "memo_field", "string", "nullable") + "," + LINE_SEPARATOR
        + String.format(FORMAT, "byte_field", "int64", "nullable") + "," + LINE_SEPARATOR
        + String.format(FORMAT, "boolean_field", "bool", "nullable") + "," + LINE_SEPARATOR
        + String.format(FORMAT, "double_field", "float64", "nullable") + "," + LINE_SEPARATOR
        + String.format(FORMAT, "currency_field", "float64", "nullable") + "," + LINE_SEPARATOR
        + String.format(FORMAT, "date_field", "timestamp", "nullable") + LINE_SEPARATOR + "]" + LINE_SEPARATOR;

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE csvtable (id INTEGER, text_field TEXT, text_field2 TEXT, memo_field MEMO, byte_field BYTE, boolean_field YESNO, double_field DOUBLE, currency_field CURRENCY, date_field DATETIME)");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable("csvtable");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCsvDump(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO csvtable (id, text_field, text_field2, memo_field, byte_field, boolean_field, double_field, currency_field, date_field) VALUES(1, 'embedded delimiter(;)', 'double-quote(\")', 'embedded newline(\n)', 2, true, 9.12345, 3.1234567, #2017-01-01 00:00:00#)");
        st.close();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        st = null;
        ResultSet rs = null;
        try {
            st = ucanaccess.createStatement();
            rs = st.executeQuery("SELECT * FROM csvtable");
            Exporter exporter = new Exporter.Builder().setDelimiter(";").build();
            exporter.dumpCsv(rs, ps);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
        }

        String actual = baos.toString(StandardCharsets.UTF_8);
        assertEquals("id;text_field;text_field2;memo_field;byte_field;boolean_field;double_field;"
            + "currency_field;date_field" + LINE_SEPARATOR + "1;\"embedded delimiter(;)\";"
            + "\"double-quote(\"\")\";embedded newline( );2;true;9.12345;3.1235;" + "2017-01-01 00:00:00"
            + LINE_SEPARATOR, actual);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDumpSchema(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            Statement st = ucanaccess.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM csvtable")) {

            Exporter exporter = new Exporter.Builder().setDelimiter(";").build();
            exporter.dumpSchema(rs, ps);
            String actual = baos.toString(StandardCharsets.UTF_8);
            assertEquals(EXPECTED_SCHEMA, actual);
        }
    }
}
