package net.ucanaccess.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.console.Main;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessConnectionBuilder;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.IThrowingSupplier;
import net.ucanaccess.util.Try;
import org.junit.jupiter.api.AfterEach;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class UcanaccessBaseTest extends AbstractBaseTest {

    private static final File     TEST_TEMP_DIR = createTempDir("ucanaccess-test");

    static {
        Main.setBatchMode(true);
    }

    private File                   fileAccDb;
    private AccessVersion          accessVersion;
    // CHECKSTYLE:OFF
    protected UcanaccessConnection ucanaccess;
    // CHECKSTYLE:ON
    private Connection             verifyConnection;

    protected UcanaccessBaseTest() {
    }

    protected final void setAccessVersion(AccessVersion _accessVersion) {
        accessVersion = _accessVersion;
    }

    protected void init(AccessVersion _accessVersion) throws SQLException {
        accessVersion = _accessVersion;
        ucanaccess = createUcanaccessConnection();
    }

    protected final AccessVersion getAccessVersion() {
        return accessVersion;
    }

    protected final FileFormat getFileFormat() {
        return accessVersion.getFileFormat();
    }

    protected final File getFileAccDb() {
        return fileAccDb;
    }

    public void checkQuery(String _query, List<List<Object>> _expected) throws SQLException {
        try (Statement st = ucanaccess.createStatement();
            ResultSet rs = st.executeQuery(_query)) {
            diff(rs, _expected, _query);
        }
    }

    public void checkQuery(String _query) throws SQLException, IOException {
        initVerifyConnection();
        try (Statement st1 = ucanaccess.createStatement();
             Statement st2 = verifyConnection.createStatement()) {

            ResultSet firstRs = st1.executeQuery(_query);
            ResultSet verifyRs = st2.executeQuery(_query);

            diffResultSets(firstRs, verifyRs, _query);
        } finally {
            if (verifyConnection != null) {
                verifyConnection.close();
                verifyConnection = null;
            }
        }
    }

    private void diff(ResultSet _resultSet, List<List<Object>> _expectedResults, String _expression) throws SQLException {
        int colCountActual = _resultSet.getMetaData().getColumnCount();
        if (_expectedResults.size() > 0) {
            assertEquals(_expectedResults.get(0).size(), colCountActual);
        }
        int rowIdx = 0;
        while (_resultSet.next()) {
            for (int col = 1; col <= colCountActual; col++) {
                assertThat(rowIdx)
                    .withFailMessage("Matrix with different length was expected: " + _expectedResults.size() + " not " + rowIdx)
                    .isLessThan(_expectedResults.size());
                Object actualObj = _resultSet.getObject(col);
                Object expectedObj = _expectedResults.get(rowIdx).get(col - 1);
                if (expectedObj == null) {
                    assertNull(actualObj);
                } else {
                    if (actualObj instanceof Blob) {

                        byte[] barrActual = Try.withResources((Blob.class.cast(actualObj))::getBinaryStream, InputStream::readAllBytes).orThrow(UncheckedIOException::new);
                        byte[] barrExpected = (byte[]) expectedObj;
                        for (int y = 0; y < barrExpected.length; y++) {
                            assertEquals(barrExpected[y], barrActual[y]);
                        }
                    } else {
                        if (actualObj instanceof Number && expectedObj instanceof Number) {
                            BigDecimal ob1b = new BigDecimal(actualObj.toString());
                            BigDecimal ob2b = new BigDecimal(expectedObj.toString());
                            actualObj = ob1b.doubleValue();
                            expectedObj = ob2b.doubleValue();
                        }
                        if (actualObj instanceof Date && expectedObj instanceof Date) {
                            actualObj = ((Date) actualObj).getTime();
                            expectedObj = ((Date) expectedObj).getTime();
                        }
                        assertEquals(expectedObj, actualObj, "Expected ob2 and ob1 to be equal at row "
                            + rowIdx + ", col " + col + " in '" + _expression + "'");
                    }
                }
            }
            rowIdx++;
        }
        assertEquals(_expectedResults.size(), rowIdx, "Matrix with different length was expected");
    }

    public void diffResultSets(ResultSet _resultSet, ResultSet _verifyResultSet, String _query) throws SQLException {
        int colCountActual = _resultSet.getMetaData().getColumnCount();
        int colCountExpected = _verifyResultSet.getMetaData().getColumnCount();
        assertEquals(colCountExpected, colCountActual);

        StringBuilder log = new StringBuilder("{");
        int row = 0;
        while (next(_verifyResultSet, _resultSet)) {
            row++;
            if (log.length() > 1) {
                log.append(",");
            }
            log.append("{");
            for (int col = 1; col <= colCountActual; col++) {
                if (col > 1) {
                    log.append(",");
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
                    assertNull(objExpected, "Object in verify set at row:col " + row + ":" + col + " should be null, but was: " + objExpected + " in [" + _query + "]");
                } else {
                    if (objActual instanceof Blob) {
                        byte[] barrActual = Try.withResources(((Blob) objActual)::getBinaryStream, InputStream::readAllBytes).orThrow(UncheckedIOException::new);
                        byte[] barrExpected = Try.withResources(((Blob) objExpected)::getBinaryStream, InputStream::readAllBytes).orThrow(UncheckedIOException::new);
                        for (int y = 0; y < barrExpected.length; y++) {
                            assertEquals(barrExpected[y], barrActual[y]);
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
                        assertEquals(objExpected, objActual);
                    }
                }
            }
            log.append("}");
        }
        log.append("}");

    }

    protected void dumpQueryResult(IThrowingSupplier<ResultSet, SQLException> _supplier) throws SQLException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true)) {
            new Main(ucanaccess, null).consoleDump(_supplier.get(), ps);
            getLogger().info("dumpQueryResult: {}", baos);
        }
    }

    protected void dumpQueryResult(String _query) throws SQLException {
        try (Statement st = ucanaccess.createStatement()) {
            ResultSet resultSet = st.executeQuery(_query);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (PrintStream ps = new PrintStream(baos, true)) {
                new Main(ucanaccess, null).consoleDump(resultSet, ps);
                getLogger().info("dumpQueryResult: {}", baos);
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
                fileAccDb = new File(TEST_TEMP_DIR, accessPath);
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
            if (suffix.isEmpty() || suffix.length() > 6) {
                suffix = ".tmp";
            }
        }
        name += new TempFileNameString() + suffix;
        return new File(TEST_TEMP_DIR, name);
    }

    /**
     * Creates a unique temporary file name using the given prefix, but does not create the file.
     * @param _prefix file name prefix
     * @return temporary file object
     */
    protected File createTempFileName(String _prefix) {
        return createTempFileName(_prefix, getFileFormat().getFileExtension());
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

        Try.catching(() -> Files.createFile(f.toPath())).orThrow(UncheckedIOException::new);

        f.deleteOnExit();
        return f;
    }

    void createNewDatabase(FileFormat _fileFormat, File _dbFile) {
        Try.withResources(() -> DatabaseBuilder.create(_fileFormat, _dbFile), Database::flush).orThrow(UncheckedIOException::new);
        getLogger().info("Access {} file created: {}", _fileFormat.name(), _dbFile.getAbsolutePath());
    }

    protected File copyResourceToTempFile(String _resourcePath) {
        File resourceFile = new File(_resourcePath);

        try (InputStream is = getClass().getClassLoader().getResourceAsStream(_resourcePath)) {
            if (is == null) {
                getLogger().warn("Resource {} not found in classpath", _resourcePath);
                return null;
            }
            File tempFile = createTempFile(resourceFile.getName().replace(".", "_"));
            getLogger().debug("Copying resource '{}' to '{}'", _resourcePath, tempFile.getAbsolutePath());
            return copyFile(is, tempFile);
        } catch (IOException _ex) {
            throw new UncheckedIOException(_ex);
        }
    }

    public int getCount(String _sql) throws SQLException {
        return getCount(_sql, true);
    }

    public int getCount(String _sql, boolean _equals) throws SQLException {
        initVerifyConnection();
        Statement st = verifyConnection.createStatement();
        ResultSet expectedResultset = st.executeQuery(_sql);
        expectedResultset.next();
        int expectedCount = expectedResultset.getInt(1);

        st = ucanaccess.createStatement();
        ResultSet actualResultset = st.executeQuery(_sql);
        actualResultset.next();
        int actualCount = actualResultset.getInt(1);

        if (_equals) {
            assertEquals(expectedCount, actualCount);
        } else {
            assertNotEquals(expectedCount, actualCount);
        }
        return expectedCount;
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
        File tempVerifyFile = createTempFile(fileAccDb.getName().replace(".", "_") + "_verify");
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

    protected final void executeStatements(String... _sqls) throws SQLException {
        try (Statement st = ucanaccess.createStatement()) {
            executeStatements(st, _sqls);
        }
    }

    protected final void executeStatements(Statement _statement, String... _sqls) throws SQLException {
        for (String sql : _sqls) {
            getLogger().info("Executing sql: {}", sql);
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
            }).orElse(e -> getLogger().warn("Database {} already closed: {}", fileAccDb, e));
        }

        if (verifyConnection != null) {
            Try.catching(() -> {
                if (!verifyConnection.isClosed()) {
                    verifyConnection.close();
                }
            }).orElse(e -> getLogger().warn("Verify connection {} already closed: {}", verifyConnection, e));
        }
    }
}
