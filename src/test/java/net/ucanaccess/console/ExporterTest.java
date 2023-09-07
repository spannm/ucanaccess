package net.ucanaccess.console;

import net.ucanaccess.test.util.AbstractTestBase;
import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.Types;

/** Unit test for {@link Exporter}. */
public class ExporterTest extends AbstractTestBase {

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

    @Test
    void testToBigQueryType() {
        assertEquals("int64", Exporter.toBigQueryType(Types.INTEGER));
        assertEquals("float64", Exporter.toBigQueryType(Types.DECIMAL));
        assertEquals("timestamp", Exporter.toBigQueryType(Types.TIMESTAMP));
        assertEquals("string", Exporter.toBigQueryType(Types.CHAR));
        assertEquals("string", Exporter.toBigQueryType(Types.VARCHAR));

        // any type not explicitly defined in the switch statement is mapped to a "string".
        assertEquals("string", Exporter.toBigQueryType(Types.BIT));
    }

    @Test
    void testToBigQueryNullable() {
        assertEquals("required", Exporter.toBigQueryNullable(0));
        assertEquals("nullable", Exporter.toBigQueryNullable(1));
        assertEquals("nullable", Exporter.toBigQueryNullable(2));
        assertEquals("nullable", Exporter.toBigQueryNullable(3));
    }

    @Test
    void testToSchemaRow() {
        assertEquals("{\"name\": \"MyName\", \"type\": \"int64\", \"mode\": \"nullable\"}",
            Exporter.toSchemaRow("MyName", Types.INTEGER, ResultSetMetaData.columnNullable));
    }
}
