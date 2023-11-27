package net.ucanaccess.console;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.log.Logger;
import net.ucanaccess.util.Try;
import net.ucanaccess.util.UcanaccessRuntimeException;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    private static final String EXPORT_USAGE = "export [--help] [--bom] [-d <delimiter>] [-t <table>] "
            + "[--big_query_schema <pathToSchemaFile>] " + "[--newlines] <pathToCsv>";

    private static final String EXPORT_PROMPT = "Export command syntax is: " + EXPORT_USAGE;

    private static boolean batchMode = false;
    private Connection     conn;
    private boolean        connected = true;
    private BufferedReader input;
    private String         lastSqlQuery;

    public Main(Connection _conn, BufferedReader _input) {
        conn = _conn;
        input = _input;
    }

    private static boolean hasPassword(File _f) throws IOException {
        try (Database db = Try.catching(() -> DatabaseBuilder.open(_f))
            .orElseGet(() -> Try.catching(() -> new DatabaseBuilder()
                .setReadOnly(true)
                .setFile(_f).open()).orThrow())) {
            return db.getDatabasePassword() != null;
        }
    }

    public static void main(String[] _args) throws Exception {
        Logger.setLogPrintWriter(new PrintWriter(System.out));
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        // password properties info
        Properties props = new Properties();
        File fl = null;
        long size = 0;
        String passwordEntry = "";
        String[] commands = null;
        if (_args.length > 0) {
            String file = _args[0];
            if (file.endsWith(".properties")) {
                File pfl = new File(_args[0]);
                if (pfl.exists()) {
                    try (FileInputStream fis = new FileInputStream(pfl)) {
                        props.load(fis);
                    }
                    // convert keys to enum name or lower-case
                    props = props.stringPropertyNames().stream()
                        .collect(Collectors.toMap(
                            k -> Optional.ofNullable(Property.parse(k)).map(Property::name).orElse(k.toLowerCase()),
                            props::getProperty, (v1, v2) -> v1, Properties::new));
                }
            } else if (file.endsWith(".accdb") || file.endsWith(".mdb")) {
                fl = new File(file);
                size = fl.length();
                if (_args.length > 1) {
                    int arg = 1;
                    if (hasPassword(fl)) {
                       passwordEntry = _args[arg++];
                    } else {
                       commands = Arrays.copyOfRange(_args, arg++, _args.length);
                    }
                }
            }
        }

        try {
            Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        } catch (ClassNotFoundException e) {

            System.err.println(e.getMessage());
            System.err.println("Check your classpath!");
            System.exit(1);
        }
        while (fl == null || !fl.exists()) {
            if (fl != null) {
                System.out.println("Given file does not exist");
            }
            System.out.print("Please, enter the full path to the access file (.mdb or .accdb): ");
            String path = input.readLine().trim();
            if (path.endsWith(";")) {
                path = path.substring(0, path.length() - 1);
            }
            if (path.equalsIgnoreCase("quit")) {
                System.out.println("I'm so unhappy. Goodbye.");
                System.exit(1);
            }
            fl = new File(path);
            size = fl.length();
        }
        Connection conn = null;
        try {
            String noMem = "";
            if (passwordEntry.isEmpty() && (props.containsKey(Property.jackcessOpener.name()) || hasPassword(fl))) {
                System.out.print("Please, enter password: ");
                passwordEntry = ";password=" + input.readLine().trim();
            }

            if (!props.containsKey(Property.jackcessOpener.name())) {
                noMem = size > 30000000 ? ";" + Property.memory + "=false" : "";
            }

            conn = DriverManager.getConnection("jdbc:ucanaccess://" + fl.getAbsolutePath() + passwordEntry + noMem, props);

            SQLWarning sqlw = conn.getWarnings();
            while (sqlw != null) {
                System.out.println(sqlw.getMessage());
                sqlw = sqlw.getNextWarning();
            }
        } catch (Exception _ex) {
            System.err.println(_ex);
            System.exit(1);
        }
        Main main = new Main(conn, input);
        main.sayHello(conn.getMetaData().getDriverVersion());
        main.start(commands);
    }

    public static void setBatchMode(boolean _batchMode) {
        batchMode = _batchMode;
    }

    /**
     * Prints the ResultSet {@code _resultSet} in a format suitable for the terminal console given by {@code _printStream}.
     */
    public void consoleDump(ResultSet _resultSet, PrintStream _printStream) throws SQLException {
        new TableFormat(_resultSet).output(_printStream);
    }

    private void executeStatement(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            if (st.execute(sql)) {
                ResultSet rs = st.getResultSet();
                if (rs != null) {
                    consoleDump(rs, System.out);
                    lastSqlQuery = sql;
                } else {
                    System.out.println("Ok!");
                }
            } else {
                int num = st.getUpdateCount();
                prompt(num == 0 ? "No rows affected" : num + " row(s) affected");
            }
        }
    }

    private void prompt() {
        System.out.println();
        if (!batchMode) {
            System.out.print("UCanAccess>");
        }
    }

    private void prompt(String content) {
        if (!batchMode) {
            System.out.println("UCanAccess>" + content);
        }
    }

    private String readInput() {
        try {
            String ret = input.readLine();
            if (ret == null) {
                prompt("Ciao!");
                System.exit(0);
            }
            return ret.trim();
        } catch (IOException _ex) {
            throw new UcanaccessRuntimeException(_ex.getMessage());
        }
    }

    private void sayHello(String version) {
        prompt("");
        System.out.println("UCanAccess version " + version);
        System.out.println("You are connected!! ");

        System.out.println("Type quit to exit ");
        System.out.println();
        System.out.println("Commands end with ; ");
        System.out.println();
        System.out.println("Use:   ");
        System.out.printf("   %s;%n", EXPORT_USAGE);
        System.out.println(
                "for exporting the result set from the last executed query or a specific table into a .csv file");
        prompt();
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
            if (userInput.equalsIgnoreCase("quit")) {
                connected = false;
                break;
            }
            sb.append(" ").append(userInput);

            // If the current userInput ends with ';', then execute the buffered command.
            if (userInput.endsWith(";")) {
                String cmd = sb.substring(0, sb.length() - 1).trim();
                try {
                    if (cmd.toLowerCase().startsWith("export ")) {
                        executeExport(cmd);
                    } else {
                        executeStatement(cmd);
                    }
                } catch (Exception _ex) {
                    prompt(_ex.getMessage());
                }
                if (exit) {
                   connected = false;
                   break;
                }
                sb = new StringBuilder();
                prompt();
            }
        }
        System.out.println("Cheers! Thank you for using the UCanAccess JDBC Driver.");
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
     * {@code -t License} option dumps the {@code License} table using the SQL statement "select * from [License]".
     */
    private void executeExport(String cmd) throws SQLException, IOException {
        List<String> tokens = tokenize(cmd);

        Exporter.Builder exporterBuilder = new Exporter.Builder();
        String table = null;
        String schemaFileName = null;

        // Process the command line flags.
        int i = 1; // skip the first token which will always be "export"
        label:
        for (; i < tokens.size(); i++) {
            String arg = tokens.get(i);
            if (!arg.startsWith("-")) {
                break;
            }
            switch (arg) {
                case "-d":
                    ++i;
                    if (i >= tokens.size()) {
                        prompt("Missing parameter for -d flag");
                        prompt(EXPORT_PROMPT);
                        return;
                    }
                    exporterBuilder.withDelimiter(tokens.get(i));
                    break;
                case "-t":
                    ++i;
                    if (i >= tokens.size()) {
                        prompt("Missing parameter for -t flag");
                        prompt(EXPORT_PROMPT);
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
                    ++i;
                    if (i >= tokens.size()) {
                        prompt("Missing parameter for --big_query_schema flag");
                        prompt(EXPORT_PROMPT);
                        return;
                    }
                    schemaFileName = tokens.get(i);
                    break;
                case "--help":
                    printExportHelp();
                    return;
                case "--":
                    ++i;
                    break label;
                default:
                    prompt("Unknown flag " + arg);
                    prompt(EXPORT_PROMPT);
                    return;
            }

        }
        if (i >= tokens.size()) {
            prompt("File name not found");
            prompt(EXPORT_PROMPT);
            return;
        }
        if (i < tokens.size() - 1) {
            prompt("Too many arguments");
            prompt(EXPORT_PROMPT);
            return;
        }
        String csvFileName = tokens.get(i);
        Exporter exporter = exporterBuilder.build();

        // Determine the SQL statement to execute. If the '-t table' option is given
        // run a 'select * from [table]' query to export the table, instead of
        // executing the 'lastSqlQuery'.
        String sqlQuery;
        if (table != null && !table.isEmpty()) {
            sqlQuery = "SELECT * FROM [" + table + "]";
        } else if (lastSqlQuery != null) {
            sqlQuery = lastSqlQuery;
        } else {
            prompt("You must first execute an SQL query, then export the ResultSet!");
            return;
        }

        exportCsvAndSchema(sqlQuery, csvFileName, schemaFileName, exporter);
    }

    /**
     * Runs the given{@code sqlQuery} and exports the CSV to {@code csvFileName}. and the BigQuery schema to
     * {@code schemaFileName} (if given). Uses the given {@code exporter} to perform the actual CSV and schema export
     * operations.
     */
    private void exportCsvAndSchema(String sqlQuery, String csvFileName, String schemaFileName, Exporter exporter)
            throws SQLException, IOException {
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(sqlQuery);

            // output the csvFile
            File csvFile = new File(csvFileName);
            try (PrintStream out = new PrintStream(csvFile)) {
                exporter.dumpCsv(rs, out);
                out.flush();
            }
            prompt("Created CSV file: " + csvFile.getAbsolutePath());

            // output the schema file if requested
            if (schemaFileName != null) {
                File schemaFile = new File(schemaFileName);
                try (PrintStream out = new PrintStream(csvFile)) {
                    exporter.dumpSchema(rs, out);
                    out.flush();
                }
                prompt("Created schema file: " + schemaFile.getAbsolutePath());
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
        private int                      maxColWidth        = 50;
        private final List<String>       colNames;
        private final List<Integer>      colWidths;
        private final List<Integer>      colTypes;
        private final List<List<String>> records            = new ArrayList<>();

        TableFormat(ResultSet _resultSet) throws SQLException {
            this(_resultSet, -1);
        }

        TableFormat(ResultSet _resultSet, int _maxRows) throws SQLException {
            ResultSetMetaData meta = _resultSet.getMetaData();
            int columnCount = meta.getColumnCount();

            colNames = new ArrayList<>(columnCount);
            colWidths = new ArrayList<>(columnCount);
            colTypes = new ArrayList<>(columnCount);
            for (int col = 1; col <= columnCount; ++col) {
                String colLabel = meta.getColumnLabel(col);
                colNames.add(colLabel);
                colWidths.add(Math.min(colLabel.length(), maxColWidth));
                colTypes.add(meta.getColumnType(col));
            }

            records.add(colNames); // header record
            while (_resultSet.next() && (_maxRows < 0 || _maxRows < records.size())) {
                List<String> rec = new ArrayList<>();
                records.add(rec);
                for (int col = 1; col <= columnCount; ++col) {
                    Object obj = _resultSet.getObject(col);
                    if (obj != null && obj.getClass().isArray() && !obj.getClass().getComponentType().isPrimitive()) {
                        obj = Arrays.toString((Object[]) obj);
                    }

                    String s = obj == null ? "" : obj.toString();
                    rec.add(s);

                    int colWidth = colWidths.get(col - 1);
                    if (colWidth < s.length() && colWidth < maxColWidth) {
                        colWidths.set(col - 1, s.length());
                    }
                }
            }
        }

        void output(PrintStream _printStream) {
            String interline = null;
            String divider = "|";
            for (int idx = 0; idx < records.size(); idx++) {
                List<String> rec = records.get(idx);
                String line = " " + joinWithLen(" " + divider + " ", rec) + " ";
                if (idx == 0) { // index 0 holds header
                    interline = line.replaceAll("[^\\" + divider + "]", "-").replace(divider, "+");
                    interline = "·" + interline + "·";
                    _printStream.println();
                    _printStream.println(interline);
                    _printStream.println(divider + line + divider);
                    _printStream.println(interline);

                } else {
                    _printStream.println(divider + line + divider);
                }
            }

            if (records.size() > 1) {
                _printStream.println(interline);
            }
        }

        String joinWithLen(CharSequence _delim, List<? extends String> _elems) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < _elems.size(); i++) {
                int width = colWidths.get(i);
                int type = colTypes.get(i);
                String str = _elems.get(i);
                sb.append(isNumericJdbcType(type) ? leftPad(str, width) : rightPad(str, width));
                if (i < _elems.size() - 1) {
                    sb.append(_delim);
                }
            }
            return sb.toString();
        }

        static boolean isNumericJdbcType(int _type) {
            return NUMERIC_JDBC_TYPES.contains(_type);
        }

        static String rightPad(String _str, int _width) {
            return (_str + repeat(" ", _width - _str.length())).substring(0, _width);
        }

        static String leftPad(String _str, int _width) {
            return (repeat(" ", _width - _str.length()) + _str).substring(0, _width);
        }

        static String repeat(String _str, int _count) {
            return _count > 0 ? String.format("%0" + _count + "d", 0).replace("0", _str) : "";
        }

    }
}
