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
 * Exports a {@link ResultSet} to different formats, primarily CSV.
 * <p>
 * The class uses a builder pattern to configure the export options before calling its dump methods.
 * <p>
 * For example, to export a {@link ResultSet} to a CSV file with a custom delimiter:
 * <pre>
 * Exporter exporter = new Exporter.Builder()
 *     .setDelimiter(",")
 *     .build();
 * ResultSet rs = ...;
 * exporter.dumpCsv(rs, System.out);
 * </pre>
 * </p>
 */
public final class Exporter {
    /** The default delimiter is semi-colon for historical reasons. */
    private static final String DEFAULT_CSV_DELIMITER = ";";

    private static final String QUOTE                 = "\"";

    // See http://unicode.org/faq/utf_bom.html#bom2
    private static final byte[] UTF8_BYTE_ORDER_MARK  = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

    private final String        delimiter;
    private final boolean       includeBom;
    private final boolean       preserveNewlines;

    /** Builder for {@link Exporter}. */
    public static class Builder {
        private String  delimiter        = DEFAULT_CSV_DELIMITER;
        private boolean includeBom       = false;
        private boolean preserveNewlines = false;

        /** * Sets the CSV column delimiter. The default is a semi-colon (";").
         *
         * @param _delimiter The delimiter character to be used.
         * @return The Builder instance for method chaining.
         */
        public Builder withDelimiter(String _delimiter) {
            delimiter = _delimiter;
            return this;
        }

        /**
         * Includes the Byte Order Mark (BOM) at the beginning of the output.
         * <p>
         * This is often needed by applications like Excel to correctly interpret UTF-8.
         *
         * @param _includeBom If {@code true}, the BOM will be included.
         * @return The Builder instance for method chaining.
         */
        public Builder includeBom(boolean _includeBom) {
            includeBom = _includeBom;
            return this;
        }

        /**
         * Preserves embedded linefeed ({@code \n}) and carriage return ({@code \r}) characters in the output.
         * <p>
         * If this is set to {@code false}, newlines will be replaced by spaces.
         *
         * @param _preverseNewlines If {@code true}, newlines are preserved.
         * @return The Builder instance for method chaining.
         */
        public Builder preserveNewlines(boolean _preverseNewlines) {
            preserveNewlines = _preverseNewlines;
            return this;
        }

        public Exporter build() {
            return new Exporter(delimiter, includeBom, preserveNewlines);
        }
    }

    private Exporter(String _delimter, boolean _includeBom, boolean _preserveNewlines) {
        delimiter = _delimter;
        includeBom = _includeBom;
        preserveNewlines = _preserveNewlines;
    }

    /**
     * Prints the ResultSet {@code _rs} in CSV format to the output stream {@code _out}.
     *
     * @param _rs The ResultSet to be exported.
     * @param _out The output stream to which the CSV data will be written.
     * @throws SQLException if a database access error occurs.
     * @throws IOException if an I/O error occurs while writing to the stream.
     */
    public void dumpCsv(ResultSet _rs, PrintStream _out) throws SQLException, IOException {

        // Print the UTF-8 byte order mark
        if (includeBom) {
            _out.write(UTF8_BYTE_ORDER_MARK);
        }

        ResultSetMetaData meta = _rs.getMetaData();
        int cols = meta.getColumnCount();

        // Print the CSV header row
        String comma = "";
        for (int i = 1; i <= cols; ++i) {
            _out.print(comma);
            _out.print(toCsv(meta.getColumnLabel(i), delimiter, preserveNewlines));
            comma = delimiter;
        }
        _out.println();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DecimalFormat decimalFormat = new DecimalFormat("0.0########");
        DecimalFormatSymbols dfs = new DecimalFormatSymbols();
        dfs.setDecimalSeparator('.');
        decimalFormat.setDecimalFormatSymbols(dfs);
        decimalFormat.setGroupingUsed(false);

        // print the resultset rows
        while (_rs.next()) {
            comma = "";
            for (int i = 1; i <= cols; ++i) {
                Object o = _rs.getObject(i);
                String str;
                if (o == null) {
                    str = "(null)";
                } else if (o.getClass().isArray()) {
                    str = Arrays.toString((Object[]) o);
                } else if (o instanceof Date) {
                    str = dateFormat.format((Date) o);
                } else if (o instanceof BigDecimal) {
                    str = decimalFormat.format(o);
                } else {
                    str = o.toString();
                }
                _out.print(comma);
                _out.print(toCsv(str, delimiter, preserveNewlines));
                comma = delimiter;
            }
            _out.println();
        }
    }

    /**
     * Prints the Google BigQuery schema of the table given by {@code _rs} in JSON format to the {@code _out} stream.<br/>
     * See <a href="https://cloud.google.com/bigquery/bq-command-line-tool">bq command-line tool</a> for a description of the JSON schema format.
     *
     * @param _rs The ResultSet whose metadata will be used to generate the schema.
     * @param _out The output stream to which the schema will be written.
     * @throws SQLException if a database access error occurs.
     */
    public void dumpSchema(ResultSet _rs, PrintStream _out) throws SQLException {
        ResultSetMetaData meta = _rs.getMetaData();
        int cols = meta.getColumnCount();
        _out.println('[');

        for (int i = 1; i <= cols; ++i) {
            String name = meta.getColumnName(i);
            int sqlType = meta.getColumnType(i);
            int nullable = meta.isNullable(i);

            _out.print(toSchemaRow(name, sqlType, nullable));
            _out.printf(i != cols ? ",%n" : "%n");
        }
        _out.println(']');
    }

    /**
     * Returns the CSV representation of the given string {@code _str}.<br/>
     * This method handles quoting and delimiter escaping according to a simplified CSV standard.
     * <ul>
     *   <li>A double-quote character (") is escaped by replacing it with two double-quotes ("").</li>
     *   <li>If the string contains the delimiter, it is enclosed in double-quotes.</li>
     *   <li>Newlines (`\r`, `\n`) are either preserved (if `_preserveNewlines` is `true`) or
     *       replaced with a space character. Preserved newlines will also cause the string to be
     *       enclosed in double-quotes.</li>
     * </ul>
     *
     * @param _str The string to be formatted as CSV.
     * @param _delimiter The delimiter character used for the CSV output.
     * @param _preserveNewlines If `true`, newline characters are kept; otherwise, they are replaced by spaces.
     * @return The CSV-formatted string.
     */
    static String toCsv(String _str, String _delimiter, boolean _preserveNewlines) {
        boolean needsTextQualifier = false;

        // A double-quote is replaced with 2 double-quotes
        if (_str.contains(QUOTE)) {
            _str = _str.replace(QUOTE, QUOTE + QUOTE);
            needsTextQualifier = true;
        }

        // If the string contains the delimiter, wrap it in quotes
        if (_str.contains(_delimiter)) {
            needsTextQualifier = true;
        }

        // Preserve or replace newlines
        if (_preserveNewlines) {
            needsTextQualifier = true;
        } else {
            _str = _str.replace('\n', ' ').replace('\r', ' ');
        }

        if (needsTextQualifier) {
            return QUOTE + _str + QUOTE;
        } else {
            return _str;
        }
    }

    /** Returns one row of the BigQuery JSON schema file. */
    static String toSchemaRow(String _name, int _sqlType, int _nullable) {
        return String.format("{\"name\": \"%s\", \"type\": \"%s\", \"mode\": \"%s\"}", _name, toBigQueryType(_sqlType),
            toBigQueryNullable(_nullable));
    }

    /**
     * Maps the {@code java.sql.Types} values to BigQuery data types.<br/>
     * We map to the BigQuery Standard SQL data types
     * (<a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types">...</a>) instead of the legacy SQL data types
     * (<a href="https://cloud.google.com/bigquery/data-types">...</a>).
     *
     * <p>
     * Any JDBC type not explicitly defined will be mapped to a BigQuery "string" type.
     *
     * @param _sqlType The {@code java.sql.Types} integer value.
     * @return The corresponding BigQuery data type as a string.
     */
    static String toBigQueryType(int _sqlType) {
        switch (_sqlType) {
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
     * Converts the {@code nullable} indicator from {@code ResultSetMetaData.isNullable()} to the equivalent BigQuery
     * schema value.
     *
     * @param _nullable The {@code ResultSetMetaData.column*Nulls} value.
     * @return The corresponding BigQuery schema value ("required" or "nullable").
     */
    static String toBigQueryNullable(int _nullable) {
        switch (_nullable) {
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
