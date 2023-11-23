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
 * Class that exports the given {@link ResultSet} to CSV format. Use the {@link Builder} to configure the Exporter
 * before calling its {@code csvDump()} method. For example:
 *
 * <pre>
 * Exporter exporter = new Exporter.Builder()
 *     .setDelimiter(",")
 *     .build();
 * ResultSet rs = ...;
 * exporter.csvDump(rs, System.out);
 * </pre>
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

        /** Sets the CSV column delimiter. */
        public Builder withDelimiter(String _delimiter) {
            delimiter = _delimiter;
            return this;
        }

        /** Includes the Byte Order Mark. Needed by Excel to read UTF-8. */
        public Builder includeBom(boolean _includeBom) {
            includeBom = _includeBom;
            return this;
        }

        /** Preserves embedded linefeed {@code \r} and carriage return {@code \n} characters. */
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
     * Prints the ResultSet {@code rs} in CSV format to the output file {@code out}.
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
     * Prints the Google BigQuery schema of the table given by {@code rs} in JSON format to the {@code out} stream. See
     * https://cloud.google.com/bigquery/bq-command-line-tool for a description of the JSON schema format.
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
     * Returns the CSV representation of the string {@code _str}.
     * <ul>
     * <li>double-quote characters (") are doubled (""), and then enclosed in double-quotes
     * <li>if the string contains the delimiter character, wrap the string in double-quotes
     * <li>preserveNewlines=false: replace newline (\n, \r) with the space character
     * <li>preserveNewlines=true: preserve newline characters by enclosing in double-quotes
     * </ul>
     * This supports only a small subset of various CSV transformations such as those given in
     * https://www.csvreader.com/csv_format.php.
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
     * Maps the {@code java.sql.Types} values to BigQuery data types. We map to the BigQuery Standard SQL data types
     * (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types) instead of the legacy SQL data types
     * (https://cloud.google.com/bigquery/data-types).
     *
     * <p>
     * Any JDBC type not explicitly defined will be mapped to a BigQuery "string" type.
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
