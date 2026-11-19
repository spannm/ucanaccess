package net.ucanaccess.console;

import static net.ucanaccess.type.SqlConstants.FROM;
import static net.ucanaccess.type.SqlConstants.SELECT;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.Database.FileFormat;
import io.github.spannm.jackcess.DatabaseBuilder;
import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Sql;
import net.ucanaccess.util.Try;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("java:S106") // interactive console that prints its results to standard output
public class Main {
    private static final String  EXPORT_USAGE  = "export [--help] [--bom] [-d <delimiter>] [-t <table>] "
        + "[--big_query_schema <pathToSchemaFile>] " + "[--newlines] <pathToCsv>";

    private static final String  EXPORT_PROMPT = "Usage: " + EXPORT_USAGE;

    private static final String  USAGE         = String.join(System.lineSeparator(),
        "Usage: console [<database> [<password> | <statement>]]",
        "       console <connection>.properties",
        "Opens an interactive SQL console on an Access database (.mdb, .accdb).",
        "  <database>    database file, asked for if missing",
        "  <password>    password of the database, asked for if needed",
        "  <statement>   SQL statement to execute instead of starting the console",
        "  --help        print this help",
        "  --version     print the version");

    private static final long    MEMORY_LIMIT  = 30_000_000;

    private static boolean       batchMode     = false;
    private final Connection     conn;
    private boolean              connected     = true;
    private final BufferedReader input;
    private String               lastSqlQuery;

    public Main(Connection conn, BufferedReader input) {
        this.conn = conn;
        this.input = input;
    }

    private static boolean hasPassword(File f) throws IOException {
        try (Database db = Try.catching(() -> DatabaseBuilder.open(f))
            .orElseGet(() -> Try.catching(() -> new DatabaseBuilder()
                .withReadOnly(true)
                .withFile(f).open()).orThrow())) {
            return db.getDatabasePassword() != null;
        }
    }

    public static void main(String[] args) throws Exception {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));
        // password properties info
        Properties props = new Properties();
        File fl = null;
        long size = 0;
        String passwordEntry = "";
        String[] commands = null;
        if (args.length > 0) {
            String file = args[0];
            if ("--help".equals(file) || "-h".equals(file)) {
                System.out.println(USAGE);
                return;
            } else if ("--version".equals(file)) {
                System.out.printf("UCanAccess %s%n", version());
                return;
            } else if (file.startsWith("-")) {
                System.err.printf("Unknown option: %s%n%s%n", file, USAGE);
                System.exit(2);
            } else if (file.toLowerCase(Locale.ROOT).endsWith(".properties")) {
                File pfl = new File(args[0]);
                if (pfl.exists()) {
                    try (FileInputStream fis = new FileInputStream(pfl)) {
                        props.load(fis);
                    }
                    // convert keys to enum name or lower-case
                    props = props.stringPropertyNames().stream()
                        .collect(Collectors.toMap(
                            k -> Optional.ofNullable(Property.parse(k)).map(Property::name).orElse(k.toLowerCase(Locale.ROOT)),
                            props::getProperty, (v1, v2) -> v1, Properties::new));
                }
            } else {
                fl = new File(file);
                size = fl.length();
                if (args.length > 1) {
                    if (fl.isFile() && hasPassword(fl)) {
                       passwordEntry = ";password=" + args[1];
                    } else {
                       commands = Arrays.copyOfRange(args, 1, args.length);
                       setBatchMode(true);
                    }
                }
            }
        }

        try {
            Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        } catch (ClassNotFoundException ex) {
            System.err.printf("UCanAccess driver not found on the classpath: %s%n", ex.getMessage());
            System.exit(1);
        }
        while (fl == null || !fl.exists()) {
            if (fl != null) {
                System.err.printf("File not found: %s%n", fl.getAbsolutePath());
            }
            System.out.print("Path to the Access database (.mdb, .accdb): ");
            String path = input.readLine();
            if (path == null) {
                System.exit(1);
            }
            path = path.trim();
            if (path.endsWith(";")) {
                path = path.substring(0, path.length() - 1);
            }
            if (isQuit(path)) {
                return;
            }
            fl = new File(path);
            size = fl.length();
        }
        Connection conn = null;
        try {
            String noMem = "";
            if (passwordEntry.isEmpty() && (props.containsKey(Property.jackcessOpener.name()) || hasPassword(fl))) {
                String password = readPassword(input);
                if (password == null) {
                    System.exit(1);
                }
                passwordEntry = ";password=" + password;
            }

            if (!props.containsKey(Property.jackcessOpener.name()) && size > MEMORY_LIMIT) {
                noMem = ";" + Property.memory + "=false";
                System.out.printf("Large database, opening it with %s=false%n", Property.memory);
            }

            conn = DriverManager.getConnection("jdbc:ucanaccess://" + fl.getAbsolutePath() + passwordEntry + noMem, props);

            SQLWarning sqlw = conn.getWarnings();
            while (sqlw != null) {
                System.out.println(sqlw.getMessage());
                sqlw = sqlw.getNextWarning();
            }
        } catch (Exception ex) {
            System.err.printf("Error: %s%n", ex.getMessage());
            System.exit(1);
        }
        Main main = new Main(conn, input);
        if (commands == null) {
            main.sayHello(fl);
        }
        main.start(commands);
    }

    /**
     * Returns the version of UCanAccess from the manifest, which is missing when running from the class files.
     */
    private static String version() {
        return Optional.ofNullable(Main.class.getPackage().getImplementationVersion()).orElse("(development build)");
    }

    /**
     * Reads the database password, without echo if a terminal is attached.
     */
    private static String readPassword(BufferedReader input) throws IOException {
        Console console = System.console();
        if (isTerminal(console)) {
            char[] password = console.readPassword("Password: ");
            return password == null ? null : new String(password);
        }
        System.out.print("Password: ");
        return Optional.ofNullable(input.readLine()).map(String::trim).orElse(null);
    }

    /**
     * Returns whether the given console is attached to a terminal. Since Java 22 {@link System#console()} returns a
     * console even without a terminal, only {@code Console.isTerminal()} tells.
     */
    private static boolean isTerminal(Console console) {
        if (console == null) {
            return false;
        }
        try {
            return (Boolean) Console.class.getMethod("isTerminal").invoke(console);
        } catch (NoSuchMethodException ex) {
            return true;
        } catch (ReflectiveOperationException ex) {
            return false;
        }
    }

    public static void setBatchMode(boolean batchMode) {
        Main.batchMode = batchMode;
    }

    /**
     * Prints the ResultSet {@code resultSet} in a format suitable for the terminal console given by {@code printStream}.
     */
    public void consoleDump(ResultSet resultSet, PrintStream printStream) throws SQLException {
        consoleDump(resultSet, printStream, false);
    }

    /**
     * Prints the ResultSet {@code resultSet} in a format suitable for the terminal console given by {@code printStream}.
     *
     * @param resultSet         result set to print
     * @param printStream       stream to print to
     * @param hideEmptyColumns  whether to leave out the columns without a value in any row, they are named below the table
     * @since 5.3.1
     */
    public void consoleDump(ResultSet resultSet, PrintStream printStream, boolean hideEmptyColumns) throws SQLException {
        new TableFormat(resultSet).hideEmptyColumns(hideEmptyColumns).output(printStream);
    }

    private void executeStatement(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            if (st.execute(sql)) {
                ResultSet rs = st.getResultSet();
                if (rs != null) {
                    consoleDump(rs, System.out);
                    lastSqlQuery = sql;
                } else {
                    System.out.println("OK");
                }
            } else {
                int num = st.getUpdateCount();
                System.out.printf("%d %s affected%n", num, num == 1 ? "row" : "rows");
            }
        }
    }

    private void prompt() {
        System.out.println();
        if (!batchMode) {
            System.out.print("UCanAccess> ");
        }
    }

    /**
     * Prints the given error message to standard error.
     */
    private static void error(String message) {
        System.err.printf("Error: %s%n", message);
    }

    private String readInput() {
        try {
            String ret = input.readLine();
            if (ret == null) {
                System.out.println();
                System.exit(0);
            }
            return ret.trim();
        } catch (IOException ex) {
            throw new UcanaccessRuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Prints the version, the database the console is connected to and how to use it.
     */
    private void sayHello(File dbFile) {
        System.out.printf("UCanAccess %s, connected to %s (%s)%n", version(), dbFile.getAbsolutePath(), fileFormat());
        System.out.println("End statements with ';'. Type 'quit' to exit, 'export --help;' for CSV export.");
        prompt();
    }

    /**
     * Returns the file format of the database, e.g. {@code Access 2007} for {@link FileFormat#V2007}.
     */
    private String fileFormat() {
        if (!(conn instanceof UcanaccessConnection)) {
            return "unknown format";
        }
        String format = Try.catching(() -> ((UcanaccessConnection) conn).getDbIO().getFileFormat().name())
            .orElse("unknown format");
        return format.matches("V\\d{4}") ? "Access " + format.substring(1) : format;
    }

    /**
     * Prints the given error message of the export command, followed by its usage.
     */
    private static void exportError(String message) {
        error(message);
        System.err.println(EXPORT_PROMPT);
    }

    /**
     * Returns whether the given input ends the console.
     */
    static boolean isQuit(String userInput) {
        String cmd = userInput.endsWith(";") ? userInput.substring(0, userInput.length() - 1).trim() : userInput;
        return "quit".equalsIgnoreCase(cmd) || "exit".equalsIgnoreCase(cmd);
    }

    private void printExportHelp() {
        System.out.printf("Usage: %s;%n", EXPORT_USAGE);
        System.out.println("Export the most recent SQL query to the given <pathToCsv> file.");
        System.out.println("  -d <delimiter> Set the CSV column delimiter (default: ';').");
        System.out.println("  -t <table>     Output the <table> instead of the previous query.");
        System.out.println("  --big_query_schema <schemaFile>  Output the BigQuery schema" + " to <schemaFile>.");
        System.out.println("  --bom          Output the UTF-8 byte order mark.");
        System.out.println("  --newlines     Preserve embedded newlines (\\r, \\n).");
        System.out.println("  --help         Print this help message.");
        System.out.println("Single (') or double (\") quoted strings are supported.");
        System.out.println("Backslash (\\) escaping (e.g. \\n, \\t) is enabled within quotes.");
        System.out.println("Use two backslashes (\\\\) to insert one backslash within quotes "
                + "(e.g. \"c:\\\\temp\\\\newfile.csv\").");
    }

    private void start(String[] commands) {
        StringBuilder sb = new StringBuilder();
        boolean exit = false;
        while (connected) {
            String userInput;
            if (commands != null) {
                userInput = String.join(" ", commands);
                if (!userInput.endsWith(";")) {
                   userInput += ";";
                }
                commands = null;
                exit = true;
            } else {
                userInput = readInput();
            }
            if (sb.length() == 0 && isQuit(userInput)) {
                connected = false;
                break;
            }
            sb.append(" ").append(userInput);

            // If the current userInput ends with ';', then execute the buffered command.
            if (userInput.endsWith(";")) {
                String cmd = sb.substring(0, sb.length() - 1).trim();
                try {
                    if (cmd.toLowerCase(Locale.ROOT).matches("export(\\s.*)?")) {
                        executeExport(cmd);
                    } else {
                        executeStatement(cmd);
                    }
                } catch (Exception ex) {
                    error(ex.getMessage());
                }
                if (exit) {
                   connected = false;
                   break;
                }
                sb.setLength(0);
                prompt();
            }
        }
    }

    /**
     * Parse the {@code cmd} to handle command line flags of the form: "export [-d delimiter] [-t table] pathToCsv". For
     * example:
     *
     * <pre>
     * export -d , -t License License.csv
     * </pre>
     *
     * The {@code -d ,} option changes the delimiter character to a comma instead of the default semicolon. The
     * {@code -t License} option dumps the {@code License} table using the SQL statement "SELECT * FROM [License]".
     */
    void executeExport(String cmd) throws SQLException, IOException {
        List<String> tokens = tokenize(cmd);

        Exporter.Builder exporterBuilder = new Exporter.Builder();
        String table = null;
        String schemaFileName = null;

        // Process the command line flags.
        int i = 1; // skip the first token which will always be "export"
        for (; i < tokens.size(); i++) {
            String arg = tokens.get(i);
            if ("--".equals(arg)) {
                i++;
                break;
            } else if (!arg.startsWith("-")) {
                break;
            }
            switch (arg) {
                case "-d":
                    i++;
                    if (i >= tokens.size()) {
                        exportError("Option -d requires a value.");
                        return;
                    }
                    exporterBuilder.withDelimiter(tokens.get(i));
                    break;
                case "-t":
                    i++;
                    if (i >= tokens.size()) {
                        exportError("Option -t requires a value.");
                        return;
                    }
                    table = tokens.get(i);
                    break;
                case "--bom":
                    exporterBuilder.includeBom(true);
                    break;
                case "--newlines":
                    exporterBuilder.preserveNewlines(true);
                    break;
                case "--big_query_schema":
                    i++;
                    if (i >= tokens.size()) {
                        exportError("Option --big_query_schema requires a value.");
                        return;
                    }
                    schemaFileName = tokens.get(i);
                    break;
                case "--help":
                    printExportHelp();
                    return;
                default:
                    exportError(String.format("Unknown option: %s", arg));
                    return;
            }

        }
        if (i >= tokens.size()) {
            exportError("Missing CSV file name.");
            return;
        }
        if (i < tokens.size() - 1) {
            exportError("Too many arguments.");
            return;
        }
        String csvFileName = tokens.get(i);
        Exporter exporter = exporterBuilder.build();

        // Determine the SQL statement to execute. If the '-t table' option is given
        // run a 'select * from [table]' query to export the table, instead of
        // executing the 'lastSqlQuery'.
        String sqlQuery;
        if (table != null && !table.isEmpty()) {
            sqlQuery = Sql.of(SELECT, "*", FROM, "[" + table + "]").toString();
        } else if (lastSqlQuery != null) {
            sqlQuery = lastSqlQuery;
        } else {
            error("Nothing to export: run a query first or use -t <table>.");
            return;
        }

        exportCsvAndSchema(sqlQuery, csvFileName, schemaFileName, exporter);
    }

    /**
     * Runs the given{@code sqlQuery} and exports the CSV to {@code csvFileName}. and the BigQuery schema to
     * {@code schemaFileName} (if given). Uses the given {@code exporter} to perform the actual CSV and schema export
     * operations.
     */
    private void exportCsvAndSchema(CharSequence sqlQuery, String csvFileName, String schemaFileName, Exporter exporter) throws SQLException, IOException {
        Objects.requireNonNull(sqlQuery, "Sql required");
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(sqlQuery.toString());

            // output the csvFile
            File csvFile = new File(csvFileName);
            try (PrintStream out = new PrintStream(csvFile, StandardCharsets.UTF_8)) {
                exporter.dumpCsv(rs, out);
                out.flush();
            }
            System.out.printf("Created CSV file: %s%n", csvFile.getAbsolutePath());

            // output the schema file if requested
            if (schemaFileName != null) {
                File schemaFile = new File(schemaFileName);
                try (PrintStream out = new PrintStream(schemaFile, StandardCharsets.UTF_8)) {
                    exporter.dumpSchema(rs, out);
                    out.flush();
                }
                System.out.printf("Created schema file: %s%n", schemaFile.getAbsolutePath());
            }
        }
    }

    static List<String> tokenize(String s) throws IOException {
        // Create a tokenizer that creates tokens delimited by whitespace. Also handles single and
        // double quotes. Does not support comments or numbers. All characters between U+0000
        // U+0020 are considered to be whitespace characters (this includes \r, \n, and \t).
        // All characters from U+0021 to U+00FF are normal word characters.
        // Internally, StreamTokenzier treats all characters >= U+0100 to be a normal
        // word character, and unfortunately, this cannot be changed.
        //
        // Non-breaking-space character U+00A0 is currently treated as a normal character,
        // but I could be convinced that it should be considered a whitespace.
        StreamTokenizer st = new StreamTokenizer(new StringReader(s));
        st.resetSyntax();
        st.wordChars(33, 255);
        st.whitespaceChars(0, ' ');
        st.quoteChar('"');
        st.quoteChar('\'');

        List<String> tokens = new ArrayList<>();
        while (true) {
            int ttype = st.nextToken();
            if (ttype == StreamTokenizer.TT_EOF) {
                break;
            } else if (ttype == StreamTokenizer.TT_WORD) {
                tokens.add(st.sval);
            } else if (ttype == '\'' || ttype == '"') {
                tokens.add(st.sval);
            }
        }
        return tokens;
    }

    /**
     * Helper to produce table-formatted output of a JDBC resultset.
     * @author Markus Spann
     */
    static class TableFormat {
        static final List<Integer>       NUMERIC_JDBC_TYPES =
            List.of(Types.BIT, Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.FLOAT,
                Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL, Types.ROWID);
        private static final int         MAX_COL_WIDTH      = 50;
        // the table is indented to set it off from the text before
        private static final String      INDENT             = "  ";
        // wider tables are printed with one block of column names and values per row
        private static final int         MAX_TABLE_WIDTH    = 200;
        // code points displayed in two console columns, after the ranges of Markus Kuhn's wcwidth
        private static final int[][]     WIDE_RANGES        = {
            {0x1100, 0x115F}, // Hangul Jamo
            {0x2E80, 0xA4CF}, // CJK radicals ... Yi
            {0xAC00, 0xD7A3}, // Hangul syllables
            {0xF900, 0xFAFF}, // CJK compatibility ideographs
            {0xFE30, 0xFE4F}, // CJK compatibility forms
            {0xFF00, 0xFF60}, // fullwidth forms
            {0xFFE0, 0xFFE6}, // fullwidth signs
            {0x1F300, 0x1F64F}, // emoji
            {0x1F900, 0x1F9FF}, // supplemental symbols and pictographs
            {0x20000, 0x3FFFD} // CJK extensions
        };

        private final Border             border;
        private final List<String>       colNames;
        private final List<Integer>      colWidths;
        private final List<Integer>      colTypes;
        private final List<List<String>> records            = new ArrayList<>();
        private boolean                  hideEmptyColumns;
        private int                      maxTableWidth      = MAX_TABLE_WIDTH;

        /**
         * Characters to draw the table border with.
         */
        enum Border {
            ASCII('|', "+-++", "+=++", "+-++", "+-++"),
            UNICODE('│', "┌─┬┐", "╞═╪╡", "├─┼┤", "└─┴┘");

            private final char   vertical;
            // each line given by its left junction, horizontal character, inner and right junction
            private final String top;
            private final String header;
            private final String middle;
            private final String bottom;

            /**
             * Creates a border of the given characters.
             *
             * @param vertical  character of the vertical lines
             * @param top       characters of the top line
             * @param header    characters of the line below the header
             * @param middle    characters of the line between two rows
             * @param bottom    characters of the bottom line
             */
            Border(char vertical, String top, String header, String middle, String bottom) {
                this.vertical = vertical;
                this.top = top;
                this.header = header;
                this.middle = middle;
                this.bottom = bottom;
            }

            /**
             * Returns the unicode border if the given charset can encode its characters, the ascii border otherwise.
             */
            static Border forCharset(Charset charset) {
                String characters = UNICODE.vertical + UNICODE.top + UNICODE.header + UNICODE.middle + UNICODE.bottom;
                return charset.canEncode() && charset.newEncoder().canEncode(characters) ? UNICODE : ASCII;
            }

            /**
             * Returns the border that the standard output can display.
             */
            static Border forConsole() {
                return forCharset(consoleCharset());
            }

            /**
             * Returns the charset of the standard output, as far as the running Java version tells it.
             */
            private static Charset consoleCharset() {
                // stdout.encoding exists since Java 19, sun.stdout.encoding on Windows consoles before, native.encoding
                // since Java 17 is what System.out uses on Java 18 when not writing to a console, sun.jnu.encoding is the
                // platform encoding on older versions, unlike the default charset not changed by -Dfile.encoding
                for (String property : new String[] {"stdout.encoding", "sun.stdout.encoding", "native.encoding", "sun.jnu.encoding"}) {
                    String name = System.getProperty(property);
                    try {
                        if (name != null && Charset.isSupported(name)) {
                            return Charset.forName(name);
                        }
                    } catch (IllegalArgumentException ignored) {
                        // an illegal charset name, try the next property
                    }
                }
                return Charset.defaultCharset();
            }

            /**
             * Returns a horizontal line of the table.
             *
             * @param chars   left junction, horizontal character, inner and right junction of the line
             * @param widths  widths of the column contents
             */
            static String line(String chars, List<Integer> widths) {
                StringJoiner line = new StringJoiner(chars.substring(2, 3), chars.substring(0, 1), chars.substring(3, 4));
                // one blank on either side of the content
                widths.forEach(w -> line.add(chars.substring(1, 2).repeat(w + 2)));
                return line.toString();
            }

            /**
             * Returns a horizontal line of the table with the given label at its start.
             *
             * @param chars   left junction, horizontal character, inner and right junction of the line
             * @param widths  widths of the column contents, the first one wide enough for the label
             * @param label   text to show in the line
             */
            static String line(String chars, List<Integer> widths, String label) {
                String line = line(chars, widths);
                String text = chars.charAt(1) + " " + label + " ";
                return line.charAt(0) + text + line.substring(1 + text.length());
            }
        }

        /**
         * Reads all rows of the given result set, drawing the border the console can display.
         *
         * @param resultSet  result set to format
         */
        TableFormat(ResultSet resultSet) throws SQLException {
            this(resultSet, -1, Border.forConsole());
        }

        /**
         * Reads the rows of the given result set.
         *
         * @param resultSet  result set to format
         * @param maxRows    maximum number of rows to read, negative for all rows
         * @param border     characters to draw the table border with
         */
        TableFormat(ResultSet resultSet, int maxRows, Border border) throws SQLException {
            this.border = border;
            ResultSetMetaData meta = resultSet.getMetaData();
            int columnCount = meta.getColumnCount();

            colNames = new ArrayList<>(columnCount);
            colWidths = new ArrayList<>(columnCount);
            colTypes = new ArrayList<>(columnCount);
            for (int col = 1; col <= columnCount; ++col) {
                String colLabel = printable(meta.getColumnLabel(col));
                colNames.add(colLabel);
                colWidths.add(Math.min(displayWidth(colLabel), MAX_COL_WIDTH));
                colTypes.add(meta.getColumnType(col));
            }

            records.add(colNames); // header record
            while ((maxRows < 0 || maxRows >= records.size()) && resultSet.next()) {
                List<String> rec = new ArrayList<>();
                records.add(rec);
                for (int col = 1; col <= columnCount; ++col) {
                    Object obj = resultSet.getObject(col);
                    if (obj != null && obj.getClass().isArray() && !obj.getClass().getComponentType().isPrimitive()) {
                        obj = Arrays.toString((Object[]) obj);
                    }

                    String s = obj == null ? "" : printable(obj.toString());
                    rec.add(s);

                    int width = Math.min(displayWidth(s), MAX_COL_WIDTH);
                    if (colWidths.get(col - 1) < width) {
                        colWidths.set(col - 1, width);
                    }
                }
            }
        }

        /**
         * Sets whether to leave out the columns without a value in any row.
         *
         * @param hide  whether to leave out empty columns
         */
        TableFormat hideEmptyColumns(boolean hide) {
            hideEmptyColumns = hide;
            return this;
        }

        /**
         * Sets the width above which the table is printed with one block of column names and values per row.
         *
         * @param width  maximum width of a table with one line per row
         */
        TableFormat maxTableWidth(int width) {
            maxTableWidth = width;
            return this;
        }

        /**
         * Prints the table to the given stream, followed by the number of rows and the hidden columns.
         *
         * @param printStream  stream to print to
         */
        void output(PrintStream printStream) {
            List<Integer> columns = visibleColumns();
            List<Integer> widths = columns.stream().map(colWidths::get).collect(Collectors.toList());
            List<List<String>> rows = records.subList(1, records.size()); // index 0 holds header

            printStream.println();
            // each column takes its width plus one blank on either side and a vertical line
            if (widths.stream().mapToInt(w -> w + 3).sum() + 1 > maxTableWidth) {
                if (rows.isEmpty()) {
                    printStream.println(INDENT + String.format("(0 rows, columns: %s)", String.join(", ", colNames)));
                } else {
                    outputVertical(printStream, rows, columns);
                }
                return;
            }
            printStream.println(INDENT + Border.line(border.top, widths));
            printStream.println(INDENT + row(colNames, columns));
            if (!rows.isEmpty()) {
                printStream.println(INDENT + Border.line(border.header, widths));
            }
            rows.forEach(r -> printStream.println(INDENT + row(r, columns)));
            printStream.println(INDENT + Border.line(border.bottom, widths));
            printStream.println(INDENT + footer(rows.size(), columns));
        }

        /**
         * Prints one block per row, listing the column names and values of the row line by line.
         *
         * @param printStream  stream to print to
         * @param rows         rows to print
         * @param columns      indexes of the columns to print
         */
        void outputVertical(PrintStream printStream, List<List<String>> rows, List<Integer> columns) {
            // the name column is wide enough for the row label in the line above each block
            String lastLabel = String.format("Row %d", rows.size());
            int nameWidth = Math.min(MAX_COL_WIDTH, columns.stream()
                .mapToInt(col -> displayWidth(colNames.get(col)))
                .reduce(lastLabel.length() + 1, Math::max));
            int valueWidth = Math.min(MAX_COL_WIDTH, rows.stream()
                .flatMap(row -> columns.stream().map(row::get))
                .mapToInt(TableFormat::displayWidth)
                .max().orElse(0));
            List<Integer> widths = List.of(nameWidth, valueWidth);

            for (int idx = 0; idx < rows.size(); idx++) {
                String label = String.format("Row %d", idx + 1);
                printStream.println(INDENT + Border.line(idx == 0 ? border.top : border.middle, widths, label));
                for (int col : columns) {
                    printStream.println(INDENT + cells(List.of(fit(colNames.get(col), nameWidth, false),
                        fit(rows.get(idx).get(col), valueWidth, false))));
                }
            }
            printStream.println(INDENT + Border.line(border.bottom, widths));
            printStream.println(INDENT + footer(rows.size(), columns));
        }

        /**
         * Returns the indexes of the columns to print, which are all columns unless empty columns are hidden. All
         * columns are printed if every one is empty.
         */
        List<Integer> visibleColumns() {
            List<Integer> all = IntStream.range(0, colNames.size()).boxed().collect(Collectors.toList());
            if (!hideEmptyColumns) {
                return all;
            }
            List<Integer> filled = all.stream()
                .filter(col -> records.stream().skip(1).anyMatch(rec -> !rec.get(col).isEmpty()))
                .collect(Collectors.toList());
            return filled.isEmpty() ? all : filled;
        }

        /**
         * Returns the line below the table with the number of rows and the names of the hidden columns.
         *
         * @param rowCount  number of rows printed
         * @param columns   indexes of the columns printed
         */
        String footer(int rowCount, List<Integer> columns) {
            StringJoiner footer = new StringJoiner(", ", "(", ")");
            footer.add(rowCount == 1 ? "1 row" : String.format("%d rows", rowCount));
            List<String> hidden = IntStream.range(0, colNames.size())
                .filter(col -> !columns.contains(col))
                .mapToObj(colNames::get)
                .collect(Collectors.toList());
            if (!hidden.isEmpty()) {
                footer.add(String.format("%d empty %s hidden: %s", hidden.size(), hidden.size() == 1 ? "column" : "columns",
                    String.join(", ", hidden)));
            }
            return footer.toString();
        }

        /**
         * Returns one row of the table with the values padded or cut to their column width.
         *
         * @param values   values of the row
         * @param columns  indexes of the columns to print
         */
        String row(List<String> values, List<Integer> columns) {
            return cells(columns.stream()
                .map(col -> fit(values.get(col), colWidths.get(col), isNumericJdbcType(colTypes.get(col))))
                .collect(Collectors.toList()));
        }

        /**
         * Returns a line of the table with the given cells between vertical lines.
         *
         * @param cells  cell contents, already fit to their column width
         */
        String cells(List<String> cells) {
            String vertical = String.valueOf(border.vertical);
            return cells.stream().collect(Collectors.joining(" " + vertical + " ", vertical + " ", " " + vertical));
        }

        /**
         * Returns whether values of the given JDBC type are aligned right.
         */
        static boolean isNumericJdbcType(int type) {
            return NUMERIC_JDBC_TYPES.contains(type);
        }

        /**
         * Replaces control characters such as line breaks and tabs and the unicode line and paragraph separators with a
         * blank, as they would break the table.
         */
        static String printable(String str) {
            return str.codePoints()
                .map(cp -> Character.isISOControl(cp) || Character.getType(cp) == Character.LINE_SEPARATOR
                    || Character.getType(cp) == Character.PARAGRAPH_SEPARATOR ? ' ' : cp)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        }

        /**
         * Returns the number of console columns the given string takes up.
         */
        static int displayWidth(String str) {
            return str.codePoints().map(TableFormat::displayWidth).sum();
        }

        /**
         * Returns the number of console columns the given code point takes up: 0 for combining marks, 2 for wide East
         * Asian characters and emoji, 1 otherwise.
         */
        static int displayWidth(int codePoint) {
            int type = Character.getType(codePoint);
            if (type == Character.NON_SPACING_MARK || type == Character.ENCLOSING_MARK || type == Character.FORMAT) {
                return 0;
            }
            return isWide(codePoint) ? 2 : 1;
        }

        /**
         * Returns whether the given code point is displayed in two console columns.
         */
        static boolean isWide(int cp) {
            if (cp == 0x303F) { // ideographic half fill space
                return false;
            }
            for (int[] range : WIDE_RANGES) {
                if (cp >= range[0] && cp <= range[1]) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Pads or cuts the given string to exactly the given display width.
         *
         * @param str         string to fit
         * @param width       display width of the result
         * @param alignRight  whether to pad on the left
         */
        static String fit(String str, int width, boolean alignRight) {
            StringBuilder sb = new StringBuilder();
            int used = 0;
            for (int cp : str.codePoints().toArray()) {
                int w = displayWidth(cp);
                if (used + w > width) {
                    break;
                }
                sb.appendCodePoint(cp);
                used += w;
            }
            String padding = " ".repeat(width - used);
            return alignRight ? padding + sb : sb + padding;
        }

    }
}
