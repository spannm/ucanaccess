/*
Copyright (c) 2017 Brian Park.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package net.ucanaccess.test.integration;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.console.Exporter;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

/**
 * Integration test for {@link net.ucanaccess.console.Exporter}.
 */
@RunWith(Parameterized.class)
public class CsvDumpTest extends AccessVersionAllTest {

    // Support both Linux and Windows.
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String FORMAT = "{\"name\": \"%s\", \"type\": \"%s\", \"mode\": \"%s\"}";

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

    public CsvDumpTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE csvtable (id INTEGER, text_field TEXT, text_field2 TEXT, memo_field MEMO, byte_field BYTE, boolean_field YESNO, double_field DOUBLE, currency_field CURRENCY, date_field DATETIME)");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("csvtable");
    }

    @Test
    public void testCsvDump() throws Exception {
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

        String actual = baos.toString("UTF-8");
        assertEquals("id;text_field;text_field2;memo_field;byte_field;boolean_field;double_field;"
                + "currency_field;date_field" + LINE_SEPARATOR + "1;\"embedded delimiter(;)\";"
                + "\"double-quote(\"\")\";embedded newline( );2;true;9.12345;3.1235;" + "2017-01-01 00:00:00"
                + LINE_SEPARATOR, actual);
    }

    @Test
    public void testDumpSchema() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        Statement st = null;
        ResultSet rs = null;
        try {
            st = ucanaccess.createStatement();
            rs = st.executeQuery("SELECT * FROM csvtable");
            Exporter exporter = new Exporter.Builder().setDelimiter(";").build();
            exporter.dumpSchema(rs, ps);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
        }
        String actual = baos.toString("UTF-8");
        assertEquals(EXPECTED_SCHEMA, actual);
    }
}
