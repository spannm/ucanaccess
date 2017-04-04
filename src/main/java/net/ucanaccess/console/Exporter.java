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
package net.ucanaccess.console;

import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Class that exports the given {@link ResultSet} to CSV format. Use the {@link Builder}
 * to configure the Exporter before calling its {@code csvDump()} method. For example:
 * <pre>
 * Exporter exporter = new Exporter.Builder()
 *     .setDelimiter(",")
 *     .build();
 * ResultSet rs = ...;
 * exporter.csvDump(rs, System.out);
 * </pre> 
 */
public class Exporter {
	// The default delimiter is semi-colon for historical reasons.
	private static final String DEFAULT_CSV_DELIMITER = ";";
	
	// See http://unicode.org/faq/utf_bom.html#bom2
	private static final byte[] UTF8_BYTE_ORDER_MARK = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
	
	private final String delimiter;
	private final boolean includeBom;
	
	/** Builder for {@link Exporter}. */
	public static class Builder {
		String delimiter = DEFAULT_CSV_DELIMITER;
		boolean includeBom = false;
		
		/** Sets the CSV column delimiter. */
		public Builder setDelimiter(String delimiter) {
			this.delimiter = delimiter;
			return this;
		}
		
		/** Includes the Byte Order Mark. Needed by Excel to read UTF-8. */
		public Builder includeBom(boolean includeBom) {
			this.includeBom = includeBom;
			return this;
		}
		
		public Exporter build() {
			return new Exporter(delimiter, includeBom);
		}
	}
	
	private Exporter(String delimter, boolean includeBom) {
		this.delimiter = delimter;
		this.includeBom = includeBom;
	}
	
	/**
	 * Prints the ResultSet {@code rs} in CSV format using the {@code delimiter} to the
	 * output file {@code out}.
	 */
	public void csvDump(ResultSet rs, PrintStream out) throws SQLException, IOException {

		// Print the UTF-8 byte order mark. 
		if (includeBom) {
			out.write(UTF8_BYTE_ORDER_MARK);
		}
		
		ResultSetMetaData meta = rs.getMetaData();
		int cols = meta.getColumnCount();
		
		// Print the CSV header row.
		String comma = "";
		for (int i = 1; i <= cols; ++i) {
			String lb=meta.getColumnLabel(i);
			out.print(comma);
			out.print(toCsv(lb, delimiter));
			comma = delimiter;
		}
		out.println();
		
		// Print the result set rows.
		while (rs.next()) {
			comma = "";
			for (int i = 1; i <= cols; ++i) {
				Object o = rs.getObject(i);
				if(o==null)o="";
				if(o!=null&&o.getClass().isArray()){
					o=Arrays.toString((Object[])o);
				}
				if(o instanceof Date){
					SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					o=df.format((Date)o);
				}
				if(o instanceof BigDecimal){
					DecimalFormat df = new DecimalFormat("0.0########");
					DecimalFormatSymbols dfs=new DecimalFormatSymbols();
					dfs.setDecimalSeparator('.');
					df.setDecimalFormatSymbols(dfs);
					df.setGroupingUsed(false);
					o=df.format(o);
					
				}
				out.print(comma);
				out.print(toCsv(o.toString(), delimiter));
				comma = delimiter;
			}
			out.println();
		}
	}
	
	/**
	 * Returns the CSV representation of the string {@code s}.
	 * <ul>
	 * <li> double-quote characters (") are doubled (""), and then enclosed in double-quotes
	 * <li> if the string contains the delimiter character, wrap the string in double-quotes
	 * <li> replace newline character with the space character
	 * </ul>
	 * This supports only a small subset of various CSV transformations such as those given in  
	 * https://www.csvreader.com/csv_format.php.
	 * 
	 * <p>TODO: Consider using a 3rd party formatter like {@code org.apache.commons.csv.CSVFormat}
	 * if we don't mind adding another dependency.
	 */
	static String toCsv(String s, String delimiter) {
		boolean needsTextQualifier = false;
		
		// A double-quote is replaced with 2 double-quotes.
		if (s.contains("\"")) {
			s = s.replace("\"",  "\"\"");
			needsTextQualifier = true;
		}
		
		// If the string contains the delimiter, then we must wrap it in quotes.
		if (s.contains(delimiter)) {
			needsTextQualifier = true;
		}
		
		// Newlines are replaced with spaces. 
		// TODO(btpark): Add an option to preserve newline characters.
		s = s.replace("\n", " ");
		
		if (needsTextQualifier) {
			return "\"" + s + "\"";
		} else {
			return s;
		}
	}
}
