package net.ucanaccess.test;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.Database.FileFormat;
import io.github.spannm.jackcess.DatabaseBuilder;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.console.Main;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessConnectionBuilder;
import net.ucanaccess.jdbc.UcanaccessStatement;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.IThrowingSupplier;
import net.ucanaccess.util.Try;
import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The project's base test holds common testing functionality and is intended for extension.
 */
public abstract class UcanaccessBaseTest extends AbstractBaseTest {

    private static final File     TEST_TEMP_DIR = createTempDir("test-ucanaccess-" + getCurrentUser());

    static {
        Main.setBatchMode(true);
    }

    private File                   fileAccDb;
    private AccessVersion          accessVersion;
    protected UcanaccessConnection ucanaccess;
    private UcanaccessConnection   verifyConnection;

    protected UcanaccessBaseTest() {
    }

    protected final void setAccessVersion(AccessVersion _accessVersion) {
        accessVersion = _accessVersion;
    }

    protected void init(AccessVersion _accessVersion) throws SQLException {
        accessVersion = _accessVersion;
        ucanaccess = createUcanaccessConnection();
    }

    protected void init() throws SQLException {
        init(null);
    }

    protected final AccessVersion getAccessVersion() {
        return accessVersion;
    }

    protected final FileFormat getFileFormat() {
        return accessVersion == null ? null : accessVersion.getFileFormat();
    }

    protected String getFileExtension() {
        return Optional.ofNullable(getFileFormat()).map(FileFormat::getFileExtension).orElse(null);
    }

    protected final File getFileAccDb() {
        return fileAccDb;
    }

    public void checkQuery(CharSequence _query, List<List<Object>> _expected) throws SQLException {
        try (UcanaccessStatement st = ucanaccess.createStatement();
            ResultSet rs = st.executeQuery(_query == null ? null : _query.toString())) {
            diff(rs, _expected, _query);
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    public void checkQuery(CharSequence _query) throws SQLException {
        initVerifyConnection();

        try (UcanaccessStatement st1 = ucanaccess.createStatement();
             UcanaccessStatement st2 = verifyConnection.createStatement()) {

            String query = _query == null ? null : _query.toString();
            ResultSet firstRs = st1.executeQuery(query);
            ResultSet verifyRs = st2.executeQuery(query);

            diffResultSets(firstRs, verifyRs, _query);
        } finally {
            if (verifyConnection != null) {
                verifyConnection.close();
                verifyConnection = null;
            }
        }
    }

    private void diff(ResultSet _resultSet, List<List<Object>> _expectedResults, CharSequence _expression) throws SQLException {
        String expressionContext = (_expression != null ? " for expression [" + _expression + "]" : "");

        // 1. read the entire ResultSet into memory
        List<List<Object>> actualResults = new ArrayList<>();
        int actualColumnCount;

        // get column count from ResultSet metadata
        ResultSetMetaData rsmd = _resultSet.getMetaData();
        actualColumnCount = rsmd.getColumnCount();

        // check the column count of expected results, if not empty
        if (!_expectedResults.isEmpty()) {
            assertEquals(_expectedResults.get(0).size(), actualColumnCount,
                "Unexpected column count in result set metadata" + expressionContext);
        }

        // read all rows from the ResultSet
        while (_resultSet.next()) {
            List<Object> actualRow = new ArrayList<>(actualColumnCount);
            for (int col = 1; col <= actualColumnCount; col++) {
                actualRow.add(_resultSet.getObject(col));
            }
            actualResults.add(actualRow);
        }

        // 2. early exit if the number of rows differs
        assertEquals(_expectedResults.size(), actualResults.size(),
            String.format("Result set size mismatch%s. Expected %d rows, but got %d rows.",
                expressionContext, _expectedResults.size(), actualResults.size()));

        // 3. perform row-by-row and column-by-column comparisons
        for (int rowIdx = 0; rowIdx < _expectedResults.size(); rowIdx++) {
            List<Object> expectedRow = _expectedResults.get(rowIdx);
            List<Object> actualRow = actualResults.get(rowIdx);

            // check column count of the current row
            assertEquals(expectedRow.size(), actualRow.size(),
                String.format("Column count mismatch at row %d%s. Expected %d columns, but got %d columns.%nExpected Row: %s%nActual Row:   %s",
                    rowIdx + 1, expressionContext, expectedRow.size(), actualRow.size(), expectedRow, actualRow));

            for (int colIdx = 0; colIdx < expectedRow.size(); colIdx++) { // 0-based for List indexing
                // column number for error messages is 1-based
                int displayCol = colIdx + 1;
                Object expectedObj = expectedRow.get(colIdx);
                Object actualObj = actualRow.get(colIdx);

                if (expectedObj == null) {
                    assertNull(actualObj,
                        String.format("Expected null at row %d, col %d%s, but got %s (type: %s).",
                            rowIdx + 1, displayCol, expressionContext, actualObj, (actualObj != null ? actualObj.getClass().getSimpleName() : "null")));
                } else {
                    if (actualObj instanceof Blob) {
                        byte[] barrActual;
                        try (InputStream is = ((Blob) actualObj).getBinaryStream()) {
                            barrActual = is.readAllBytes();
                        } catch (IOException e) {
                            throw new SQLException("Failed to read Blob content at row " + (rowIdx + 1) + ", col " + displayCol + expressionContext, e);
                        }
                        byte[] barrExpected = (byte[]) expectedObj; // Casting to byte[] as expectedObj for Blob must be byte[]

                        assertEquals(barrExpected.length, barrActual.length,
                            String.format("Blob length mismatch at row %d, col %d%s.%nExpected length: %d%nActual length:   %d",
                                rowIdx + 1, displayCol, expressionContext, barrExpected.length, barrActual.length));

                        for (int y = 0; y < barrExpected.length; y++) {
                            assertEquals(barrExpected[y], barrActual[y],
                                String.format("Blob content mismatch at row %d, col %d, byte position %d%s.%nExpected: %d%nActual:   %d",
                                    rowIdx + 1, displayCol, y, expressionContext, barrExpected[y], barrActual[y]));
                        }
                    } else if (actualObj instanceof Double || actualObj instanceof Float ||
                               actualObj instanceof BigDecimal || expectedObj instanceof Double ||
                               expectedObj instanceof Float || expectedObj instanceof BigDecimal) {

                        BigDecimal bdActual;
                        BigDecimal bdExpected;

                        // create BigDecimal from the actual value
                        if (actualObj instanceof BigDecimal) {
                            bdActual = (BigDecimal) actualObj;
                        } else if (actualObj instanceof Number) {
                            bdActual = new BigDecimal(actualObj.toString());
                        } else {
                            fail(String.format("Actual object of unexpected type for numeric comparison at row %d, col %d%s: %s (type: %s)",
                                rowIdx + 1, displayCol, expressionContext, actualObj, (actualObj != null ? actualObj.getClass().getName() : "null")));
                            continue;
                        }

                        // create BigDecimal from the expected value
                        if (expectedObj instanceof BigDecimal) {
                            bdExpected = (BigDecimal) expectedObj;
                        } else if (expectedObj instanceof Number) {
                            bdExpected = new BigDecimal(expectedObj.toString());
                        } else {
                            fail(String.format("Expected object of unexpected type for numeric comparison at row %d, col %d%s: %s (type: %s)",
                                rowIdx + 1, displayCol, expressionContext, expectedObj, (expectedObj != null ? expectedObj.getClass().getSimpleName() : "null")));
                            continue;
                        }

                        final int SCALE = 3;
                        final double DELTA = 0.001; // small tolerance for rounding errors

                        double roundedActual = bdActual.setScale(SCALE, RoundingMode.HALF_UP).doubleValue();
                        double roundedExpected = bdExpected.setScale(SCALE, RoundingMode.HALF_UP).doubleValue();

                        assertEquals(roundedExpected, roundedActual, DELTA,
                            String.format("Expected floating-point value to be equal with tolerance at row %d, col %d%s.%nExpected: %s (%s)%nActual:   %s (%s)",
                                rowIdx + 1, displayCol, expressionContext,
                                expectedObj, (expectedObj != null ? expectedObj.getClass().getSimpleName() : "null"),
                                actualObj, (actualObj != null ? actualObj.getClass().getSimpleName() : "null")));

                    } else if (actualObj instanceof Number && expectedObj instanceof Number) {
                        // handle integral number types (e.g., Integer vs. Long)
                        // This block should come *before* the generic else, but *after* the floating-point/BigDecimal specific one.
                        // It specifically targets situations like Integer vs Long, Short vs Integer etc.
                        assertEquals(((Number) expectedObj).longValue(), ((Number) actualObj).longValue(),
                            String.format("Expected integral number to be equal (converted to long) at row %d, col %d%s.%nExpected: %s (%s)%nActual:   %s (%s)",
                                rowIdx + 1, displayCol, expressionContext,
                                expectedObj, (expectedObj != null ? expectedObj.getClass().getSimpleName() : "null"),
                                actualObj, (actualObj != null ? actualObj.getClass().getSimpleName() : "null")));

                    } else if (actualObj instanceof Date && expectedObj instanceof Date) {
                        assertEquals(((Date) expectedObj).getTime(), ((Date) actualObj).getTime(),
                            String.format("Expected Date objects to be equal at row %d, col %d%s.%nExpected: %s%nActual:   %s",
                                rowIdx + 1, displayCol, expressionContext, expectedObj, actualObj));
                    } else {
                        // For all other types (e.g., Integer, Long, String, Boolean)
                        assertEquals(expectedObj, actualObj,
                            String.format("Expected objects to be equal at row %d, col %d%s.%nExpected: %s (%s)%nActual:   %s (%s)",
                                rowIdx + 1, displayCol, expressionContext,
                                expectedObj, (expectedObj != null ? expectedObj.getClass().getSimpleName() : "null"),
                                actualObj, (actualObj != null ? actualObj.getClass().getSimpleName() : "null")));
                    }
                }
            }
        }
    }

    private void diffResultSets(ResultSet _resultSet, ResultSet _verifyResultSet, CharSequence _query) throws SQLException {
        int colCountActual = _resultSet.getMetaData().getColumnCount();
        int colCountExpected = _verifyResultSet.getMetaData().getColumnCount();
        assertEquals(colCountExpected, colCountActual, "Unexpected column count");

        StringBuilder log = new StringBuilder('{');
        int row = 0;
        while (next(_verifyResultSet, _resultSet)) {
            row++;
            if (log.length() > 1) {
                log.append(',');
            }
            log.append('{');
            for (int col = 1; col <= colCountActual; col++) {
                if (col > 1) {
                    log.append(',');
                }
                Object objActual = _resultSet.getMetaData().getColumnType(col) == Types.BOOLEAN
                    ? _resultSet.getBoolean(col)
                    : _resultSet.getObject(col);
                Object objExpected = _verifyResultSet.getMetaData().getColumnType(col) == Types.BOOLEAN
                    ? _verifyResultSet.getBoolean(col)
                    : _verifyResultSet.getObject(col);

                if (objActual == null && objExpected == null) {
                    continue;
                } else if (objActual == null) {
                    assertNull(objExpected, "Object in verify set at row:col " + row + ':' + col + " should be null, but was: "
                        + objExpected + " in [" + _query + ']');
                } else {
                    if (objActual instanceof Blob) {
                        byte[] barrActual = Try.withResources(((Blob) objActual)::getBinaryStream, InputStream::readAllBytes).orThrow(UncheckedIOException::new);
                        byte[] barrExpected = Try.withResources(((Blob) objExpected)::getBinaryStream, InputStream::readAllBytes).orThrow(UncheckedIOException::new);
                        for (int y = 0; y < barrExpected.length; y++) {
                            assertEquals(barrExpected[y], barrActual[y], "Byte mismatch at position " + y + " in column " + col + " in blob");
                        }
                    } else if (objActual instanceof ComplexBase[] && objExpected instanceof ComplexBase[]) {
                        assertArrayEquals((ComplexBase[]) objExpected, (ComplexBase[]) objActual);
                    } else {
                        if (objActual instanceof Number && objExpected instanceof Number) {
                            BigDecimal ob1b = new BigDecimal(objActual.toString());
                            BigDecimal ob2b = new BigDecimal(objExpected.toString());
                            objActual = ob1b.doubleValue();
                            objExpected = ob2b.doubleValue();
                        }
                        if (objActual instanceof Date && objExpected instanceof Date) {
                            objActual = ((Date) objActual).getTime();
                            objExpected = ((Date) objExpected).getTime();
                        }
                        assertEquals(objExpected, objActual, "Content mismatch in column " + col);
                    }
                }
            }
            log.append('}');
        }
        log.append('}');

    }

    protected void dumpQueryResult(IThrowingSupplier<ResultSet, SQLException> _supplier) throws SQLException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true)) {
            new Main(ucanaccess, null).consoleDump(_supplier.get(), ps);
            getLogger().log(Level.INFO, "dumpQueryResult: {0}", baos);
        }
    }

    protected void dumpQueryResult(CharSequence _query) throws SQLException {
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            ResultSet resultSet = st.executeQuery(_query == null ? null : _query.toString());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (PrintStream ps = new PrintStream(baos, true)) {
                new Main(ucanaccess, null).consoleDump(resultSet, ps);
                getLogger().log(Level.INFO, "dumpQueryResult: {0}", baos);
            }
        }
    }

    /**
     * Subclasses may provide their own test database by overriding this method.
     *
     * @return valid classpath reference to a test database
     */
    protected String getAccessPath() {
        return null;
    }

    protected final String getAccessTempPath() {
        String accessPath = getAccessPath();
        if (accessPath == null) {
            fileAccDb = createTempFile(getClass().getSimpleName() + '-');
            createNewDatabase(getFileFormat(), fileAccDb);
        } else {
            fileAccDb = copyResourceToTempFile(accessPath);
            if (fileAccDb == null) {
                fileAccDb = new File(getTestTempDir(), accessPath);
                if (!fileAccDb.exists()) {
                    createNewDatabase(getFileFormat(), fileAccDb);
                    fileAccDb.deleteOnExit();
                }
            }
        }

        return fileAccDb.getAbsolutePath();
    }

    protected static final String getTestDbDir() {
        return "testdbs/";
    }

    /**
     * Determines the current logged on user.
     *
     * @return logged on user
     */
    static String getCurrentUser() {
        return Stream.of("user.name", "USER", "USERNAME")
            .map(System::getProperty)
            .filter(s -> !s.isBlank())
            .findFirst().orElse(null);
    }

    protected static final File getTestTempDir() {
        return TEST_TEMP_DIR;
    }

    /**
     * Unique string based on current date/time to be used in names of temporary files.
     */
    private static final class TempFileNameString {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
        private static final AtomicInteger     COUNTER   = new AtomicInteger(1);
        private final String                   name;

        private TempFileNameString() {
            name = LocalDateTime.now().format(FORMATTER) + '_' + String.format("%03d", COUNTER.getAndIncrement());
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Creates a unique temporary file name using the given prefix and suffix, but does not create the file.
     * @param _prefix file name prefix
     * @param _suffix file name suffix
     * @return temporary file object
     */
    protected static File createTempFileName(String _prefix, String _suffix) {
        String name = Optional.ofNullable(_prefix).map(p -> p.replace(File.separatorChar, '_')).orElse("");
        if (!name.isBlank() && !name.endsWith("-")) {
            name += "-";
        }
        String suffix = _suffix;
        if (suffix == null || suffix.isBlank()) {
            int idxLastDot = _prefix.lastIndexOf('.');
            if (idxLastDot > -1) {
                suffix = _prefix.substring(idxLastDot);
            }
            if (suffix == null || suffix.isEmpty() || suffix.length() > 6) {
                suffix = ".tmp";
            }
        }
        name += new TempFileNameString() + suffix;
        return new File(getTestTempDir(), name);
    }

    /**
     * Creates a unique temporary file name using the given prefix, but does not create the file.
     * @param _prefix file name prefix
     * @return temporary file object
     */
    protected File createTempFileName(String _prefix) {
        return createTempFileName(_prefix, getFileExtension());
    }

    /**
     * Creates a unique temporary file using the given prefix.<p>
     * The file is marked for deletion on JVM exit.
     *
     * @param _prefix file name prefix
     * @return temporary file object
     */
    protected File createTempFile(String _prefix) {
        File f = createTempFileName(_prefix);

        getLogger().log(Level.DEBUG, "Creating temp file {0}", f);
        Try.catching(() -> Files.createFile(f.toPath())).orThrow(UncheckedIOException::new);

        f.deleteOnExit();
        return f;
    }

    void createNewDatabase(FileFormat _fileFormat, File _dbFile) {
        Try.withResources(() -> DatabaseBuilder.create(_fileFormat, _dbFile), Database::flush).orThrow(UncheckedIOException::new);
        getLogger().log(Level.INFO, "Access {0} database created: {1}", _fileFormat.name(), _dbFile.getAbsolutePath());
    }

    protected File copyResourceToTempFile(String _resourcePath) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(_resourcePath)) {
            if (is == null) {
                getLogger().log(Level.WARNING, "Resource {0} not found in classpath", _resourcePath);
                return null;
            }
            File tempFile = createTempFile(new File(_resourcePath).getName().replace('.', '_'));
            getLogger().log(Level.DEBUG, "Copying resource {0} to {1}", _resourcePath, tempFile.getAbsolutePath());
            return copyFile(is, tempFile);
        } catch (IOException _ex) {
            throw new UncheckedIOException(_ex);
        }
    }

    public int getVerifyCount(CharSequence _sql) throws SQLException {
        return getVerifyCount(_sql, true);
    }

    public int getVerifyCount(CharSequence _sql, boolean _equals) throws SQLException {
        String sql = _sql == null ? null : _sql.toString();
        initVerifyConnection();

        try (UcanaccessStatement stVerify = verifyConnection.createStatement();
             UcanaccessStatement stActual = ucanaccess.createStatement()) {
            ResultSet expectedRs = stVerify.executeQuery(sql);
            expectedRs.next();
            int expectedCount = expectedRs.getInt(1);

            ResultSet actualRs = stActual.executeQuery(sql);
            actualRs.next();
            int actualCount = actualRs.getInt(1);

            if (_equals) {
                assertEquals(expectedCount, actualCount);
            } else {
                assertNotEquals(expectedCount, actualCount);
            }
            return expectedCount;
        }
    }

    public String getName() {
        return getClass().getSimpleName() + " ver " + getFileFormat();
    }

    /**
     * Returns a pre-configured connection builder using {@link #getAccessTempPath()} as database path.
     * @return connection builder
     */
    protected final UcanaccessConnection createUcanaccessConnection() {
        return buildConnection()
            .withDbPath(getAccessTempPath())
            .build();
    }

    /**
     * Returns a pre-configured connection builder.
     * @return connection builder
     */
    protected UcanaccessConnectionBuilder buildConnection() {
        return new UcanaccessConnectionBuilder()
            .withUser("ucanaccess");
    }

    protected void initVerifyConnection() throws SQLException {
        File tempVerifyFile = createTempFile(fileAccDb.getName().replace('.', '_') + "_verify");
        copyFile(fileAccDb.toPath(), tempVerifyFile);

        if (verifyConnection != null) {
            verifyConnection.close();
        }
        verifyConnection = buildConnection().withDbPath(tempVerifyFile.getAbsolutePath()).build();
    }

    private boolean next(ResultSet _verifyResultSet, ResultSet _resultSet) throws SQLException {
        boolean b1 = _verifyResultSet.next();
        boolean b2 = _resultSet.next();
        assertEquals(b1, b2);
        return b1;
    }

    protected final void executeStatements(CharSequence... _sqls) throws SQLException {
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            executeStatements(st, _sqls);
        }
    }

    protected final void executeStatements(Statement _statement, CharSequence... _sqls) throws SQLException {
        for (CharSequence s : _sqls) {
            String sql = s == null ? null : s.toString();
            getLogger().log(Level.INFO, "Executing sql: {0}", sql.length() > 500 ? sql.substring(0, 500) + "..." : sql);
            _statement.execute(sql);
        }
    }

    /**
     * A single record made up of one column.
     */
    protected static final List<Object> rec(Object _col) {
        List<Object> rec = new ArrayList<>();
        rec.add(_col);
        return rec;
    }

    /**
     * A single record made up of one or more columns.
     */
    @SafeVarargs
    protected static final List<Object> rec(Object... _cols) {
        return Stream.of(_cols).collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * A list of a single record made up of one or more columns.
     */
    protected static List<List<Object>> singleRec(Object... _cols) {
        return recs(rec(_cols));
    }

    /**
     * A list of records.
     */
    @SafeVarargs
    protected static final List<List<Object>> recs(List<Object>... _recs) {
        return Stream.of(_recs).collect(Collectors.toCollection(ArrayList::new));
    }

    @AfterEach
    protected final void afterTestCaseBase() {
        if (ucanaccess != null) {
            Try.catching(() -> {
                if (!ucanaccess.isClosed()) {
                    ucanaccess.close();
                }
            }).orElse(e -> getLogger().log(Level.WARNING, "Database {0} already closed: {1}", fileAccDb, e));
        }

        if (verifyConnection != null) {
            Try.catching(() -> {
                if (!verifyConnection.isClosed()) {
                    verifyConnection.close();
                }
            }).orElse(e -> getLogger().log(Level.WARNING, "Verify connection {0} already closed: {1}", verifyConnection, e));
        }
    }
}
