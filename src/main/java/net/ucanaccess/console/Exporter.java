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
import java.sql.Types;
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
	private final boolean preserveNewlines;
	
	/** Builder for {@link Exporter}. */
	public static class Builder {
		String delimiter = DEFAULT_CSV_DELIMITER;
		boolean includeBom = false;
		boolean preserveNewlines = false;
		
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
		
		/** Preserves embedded linefeed (\r) and carriage return (\n) characters. */
		public Builder preserveNewlines(boolean preverseNewlines) {
			this.preserveNewlines = preverseNewlines;
			return this;
		}
		
		public Exporter build() {
			return new Exporter(delimiter, includeBom, preserveNewlines);
		}
	}
	
	private Exporter(String delimter, boolean includeBom, boolean preserveNewlines) {
		this.delimiter = delimter;
		this.includeBom = includeBom;
		this.preserveNewlines = preserveNewlines;
	}
	
	/**
	 * Prints the ResultSet {@code rs} in CSV format to the output file {@code out}.
	 */
	public void dumpCsv(ResultSet rs, PrintStream out) throws SQLException, IOException {

		// Print the UTF-8 byte order mark. 
		if (includeBom) {
			out.write(UTF8_BYTE_ORDER_MARK);
		}
		
		ResultSetMetaData meta = rs.getMetaData();
		int cols = meta.getColumnCount();
		
		// Print the CSV header row.
		String comma = "";
		for (int i = 1; i <= cols; ++i) {
			String lb = meta.getColumnLabel(i);
			out.print(comma);
			out.print(toCsv(lb, delimiter, false /* preserveNewlines */));
			comma = delimiter;
		}
		out.println();

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DecimalFormat decimalFormat = new DecimalFormat("0.0########");
		DecimalFormatSymbols dfs = new DecimalFormatSymbols();
		dfs.setDecimalSeparator('.');
		decimalFormat.setDecimalFormatSymbols(dfs);
		decimalFormat.setGroupingUsed(false);

		// Print the result set rows.
		while (rs.next()) {
			comma = "";
			for (int i = 1; i <= cols; ++i) {
				Object o = rs.getObject(i);
				if (o == null) {
					// TODO: Distinguish between a null string and an empty string.
					o = "";
				} else if (o.getClass().isArray()) {
					o = Arrays.toString((Object[]) o);
				} else if (o instanceof Date){
					o = dateFormat.format((Date) o);
				} else if (o instanceof BigDecimal) {
					o = decimalFormat.format(o);
				}
				out.print(comma);
				out.print(toCsv(o.toString(), delimiter, preserveNewlines));
				comma = delimiter;
			}
			out.println();
		}
	}
	
	/**
	 * Prints the Google BigQuery schema of the table given by {@code rs} in JSON format to the
	 * {@code out} stream. See https://cloud.google.com/bigquery/bq-command-line-tool for a 
	 * description of the JSON schema format.
	 */
	public void dumpSchema(ResultSet rs, PrintStream out) throws SQLException, IOException {
		ResultSetMetaData meta = rs.getMetaData();
		int cols = meta.getColumnCount();
		out.println("[");;
		for (int i = 1; i <= cols; ++i) {
			String name = meta.getColumnName(i);
			int sqlType = meta.getColumnType(i);
			int nullable = meta.isNullable(i);
			
			out.print(toSchemaRow(name, sqlType, nullable));
			out.printf((i != cols) ? ",%n": "%n");
		}
		out.println("]");;
	}

	/**
	 * Returns the CSV representation of the string {@code s}.
	 * <ul>
	 * <li> double-quote characters (") are doubled (""), and then enclosed in double-quotes
	 * <li> if the string contains the delimiter character, wrap the string in double-quotes
	 * <li> preserveNewlines=false: replace newline (\n, \r) with the space character 
	 * <li> preserveNewlines=true: preserve newline characters by enclosing in double-quotes
	 * </ul>
	 * This supports only a small subset of various CSV transformations such as those given in  
	 * https://www.csvreader.com/csv_format.php.
	 * 
	 * <p>TODO: Consider using a 3rd party formatter like {@code org.apache.commons.csv.CSVFormat}
	 * if we don't mind adding another dependency.
	 */
	static String toCsv(String s, String delimiter, boolean preserveNewlines) {
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
		
		// Preserve or replace newlines.
		if (preserveNewlines) {
			needsTextQualifier = true;
		} else {
			s = s.replace("\n", " ").replace("\r", " ");
		}
		
		if (needsTextQualifier) {
			return "\"" + s + "\"";
		} else {
			return s;
		}
	}	

	/** Returns one row of the BigQuery JSON schema file. */
	static String toSchemaRow(String name, int sqlType, int nullable) {
		return String.format("{\"name\": \"%s\", \"type\": \"%s\", \"mode\": \"%s\"}",
				name, // TODO: Do we need to escape special characters?
				toBigQueryType(sqlType),
				toBigQueryNullable(nullable));
	}

	/**
	 * Maps the {@code java.sql.Types} values to BigQuery data types.
	 * We map to the BigQuery Standard SQL data types 
	 * (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
	 * instead of the legacy SQL data types
	 * (https://cloud.google.com/bigquery/data-types).
	 *
	 * <p>Any JDBC type not explicitly defined will be mapped to a BigQuery "string" type.
	 */
	static String toBigQueryType(int sqlType) {
		switch (sqlType) {
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
			return "int64";
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.NUMERIC:
		case Types.DECIMAL:
			return "float64";
		case Types.TIMESTAMP:
			return "timestamp";
		case Types.BOOLEAN:
			return "bool";
		default:
			return "string";
		}
	}
	
	/**
	 * Converts the {@code nullable} indicator from {@code ResultSetMetaData.isNullable()} to the
	 * equivalent BigQuery schema value.
	 */
	static String toBigQueryNullable(int nullable) {
		switch (nullable) {
		case ResultSetMetaData.columnNoNulls:
			return "required";
		case ResultSetMetaData.columnNullable:
		case ResultSetMetaData.columnNullableUnknown:
			return "nullable";
		default:
			return "nullable";
		}
	}
}
