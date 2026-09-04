package net.ucanaccess.console;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.ResultSetMetaData;
import java.sql.Types;

class ExporterTest extends AbstractBaseTest {

    @Test
    void testToCsvReplacingNewlines() {
        boolean preserveNewlines = false;
        assertEquals("\"a,b\"", Exporter.toCsv("a,b", ",", preserveNewlines));
        assertEquals("\"a,,b\"", Exporter.toCsv("a,,b", ",", preserveNewlines));
        assertEquals("\"a\"\"b\"", Exporter.toCsv("a\"b", ",", preserveNewlines));
        assertEquals("a  b", Exporter.toCsv("a\r\nb", ",", preserveNewlines));
        assertEquals("a\tb", Exporter.toCsv("a\tb", ",", preserveNewlines));
        assertEquals("a'b'c", Exporter.toCsv("a'b'c", ",", preserveNewlines));
    }

    @Test
    void testToCsvPreservingNewlines() {
        boolean preserveNewlines = true;
        assertEquals("\"a\r\nb\"", Exporter.toCsv("a\r\nb", ",", preserveNewlines));
    }

    @ParameterizedTest(name = "[{index}] {0} --> {1}")
    @CsvSource(delimiter = ';', value = {
        Types.CHAR + "; string",
        Types.DECIMAL + "; float64",
        Types.INTEGER + "; int64",
        Types.TIMESTAMP + "; timestamp",
        Types.VARCHAR + "; string",
        // any type not explicitly defined in the switch statement is mapped to a "string".
        Types.BIT + "; string"
    })
    void testToBigQueryType(int sqlType, CharSequence expected) {
        assertEquals(expected, Exporter.toBigQueryType(sqlType));
    }

    @ParameterizedTest(name = "[{index}] {0} --> {1}")
    @CsvSource(delimiter = ';', value = {
        "0; required",
        "1; nullable",
        "2; nullable",
        "3; nullable"
    })
    void testToBigQueryNullable(int nullable, CharSequence expected) {
        assertEquals(expected, Exporter.toBigQueryNullable(nullable));
    }

    @Test
    void testToSchemaRow() {
        assertEquals("{\"name\": \"MyName\", \"type\": \"int64\", \"mode\": \"nullable\"}",
            Exporter.toSchemaRow("MyName", Types.INTEGER, ResultSetMetaData.columnNullable));
    }
}
