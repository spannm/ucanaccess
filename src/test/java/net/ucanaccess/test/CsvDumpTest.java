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
package net.ucanaccess.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

import net.ucanaccess.console.Exporter;

/**
 * Unit test for {@link net.ucanaccess.console.Main#csvDump()}.
 */
public class CsvDumpTest extends UcanaccessTestBase {
	// Support both Linux and Windows.
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");
	
	public CsvDumpTest() {
		super();
	}

	public CsvDumpTest(FileFormat accVer) {
		super(accVer);
	}

	public void testCsvDump() throws Exception {
		Statement st = ucanaccess.createStatement();
		st.execute("CREATE TABLE csvtable ("
				+ "id INTEGER, "
				+ "text_field TEXT, "
				+ "text_field2 TEXT, "
				+ "memo_field MEMO, "
				+ "byte_field BYTE, "
				+ "boolean_field YESNO, "
				+ "double_field DOUBLE, "
				+ "currency_field CURRENCY, "
				+ "date_field DATETIME)");
		st.close();

		st = ucanaccess.createStatement();
		st.execute("INSERT INTO csvtable ("
				+ "id, "
				+ "text_field, "
				+ "text_field2, "
				+ "memo_field, "
				+ "byte_field, "
				+ "boolean_field, "
				+ "double_field, "
				+ "currency_field, "
				+ "date_field) "
				+ "VALUES("
				+ "1, "
				+ "'embedded delimiter(;)', "
				+ "'double-quote(\")', "
				+ "'embedded newline(\n)', "
				+ "2, "
				+ "true, "
				+ "9.12345, "
				+ "3.1234567, "
				+ "#2017-01-01 00:00:00#)");
		st.close();

		String expectedCsv =
				"id;"
				+ "text_field;"
				+ "text_field2;"
				+ "memo_field;"
				+ "byte_field;"
				+ "boolean_field;"
				+ "double_field;"
				+ "currency_field;"
				+ "date_field"
				+ LINE_SEPARATOR
				+ "1;"
				+ "\"embedded delimiter(;)\";"
				+ "\"double-quote(\"\")\";"
				+ "embedded newline( );"
				+ "2;"
				+ "true;"
				+ "9.12345;"
				+ "3.1235;" // only 4 digits allowed in Currency
				+ "2017-01-01 00:00:00"
				+ LINE_SEPARATOR;
		csvDumpVerify(expectedCsv, "SELECT * FROM csvtable");
	}

	/**
	 * Verifies that the CSV dump of the {@code select} statement is equal to {@code expected}.
	 */
	private void csvDumpVerify(String expected, String select)
			throws IOException, SQLException, UnsupportedEncodingException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);

		Statement st = null;
		ResultSet rs = null;
		try {
			st = ucanaccess.createStatement();
			rs = st.executeQuery(select);
			Exporter exporter = new Exporter.Builder()
					.setDelimiter(";")
					.build();
			exporter.csvDump(rs, ps);
		} finally {
			if (rs != null)
				rs.close();
			if (st != null)
				st.close();
		}

		assertEquals(expected, baos.toString("UTF-8"));
	}
}
