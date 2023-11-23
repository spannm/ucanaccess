package net.ucanaccess.jdbc;

import net.ucanaccess.console.Exporter;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
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

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE csvtable (id INTEGER, text_field TEXT, text_field2 TEXT, memo_field MEMO, "
            + "byte_field BYTE, boolean_field YESNO, double_field DOUBLE, currency_field CURRENCY, date_field DATETIME)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCsvDump(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO csvtable (id, text_field, text_field2, memo_field, byte_field, boolean_field, double_field, currency_field, date_field) "
                + "VALUES(1, 'embedded delimiter(;)', 'double-quote(\")', 'embedded newline(\n)', 2, true, 9.12345, 3.1234567, #2017-01-01 00:00:00#)");
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            Statement st = ucanaccess.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM csvtable")) {

            new Exporter.Builder().withDelimiter(";").build().dumpCsv(rs, ps);

            String actual = baos.toString(StandardCharsets.UTF_8);
            assertEquals("id;text_field;text_field2;memo_field;byte_field;boolean_field;double_field;"
                + "currency_field;date_field" + System.lineSeparator()
                + "1;\"embedded delimiter(;)\";\"double-quote(\"\")\";embedded newline( );2;true;9.12345;3.1235;" + "2017-01-01 00:00:00" + System.lineSeparator(),
                actual);
        }

    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDumpSchema(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            Statement st = ucanaccess.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM csvtable")) {

            Exporter exporter = new Exporter.Builder().withDelimiter(";").build();
            exporter.dumpSchema(rs, ps);
            String actual = baos.toString(StandardCharsets.UTF_8);

            String format = "{\"name\": \"%s\", \"type\": \"%s\", \"mode\": \"%s\"}";
            String expectedSchema = "[" + System.lineSeparator()
                + String.join("," + System.lineSeparator(),
                    String.format(format, "id", "int64", "nullable"),
                    String.format(format, "text_field", "string", "nullable"),
                    String.format(format, "text_field2", "string", "nullable"),
                    String.format(format, "memo_field", "string", "nullable"),
                    String.format(format, "byte_field", "int64", "nullable"),
                    String.format(format, "boolean_field", "bool", "nullable"),
                    String.format(format, "double_field", "float64", "nullable"),
                    String.format(format, "currency_field", "float64", "nullable"),
                    String.format(format, "date_field", "timestamp", "nullable"))
                + System.lineSeparator() + "]" + System.lineSeparator();

            assertEquals(expectedSchema, actual);
        }
    }
}
