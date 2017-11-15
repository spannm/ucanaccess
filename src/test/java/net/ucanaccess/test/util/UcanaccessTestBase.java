/*
Copyright (c) 2012 Marco Amadei.

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
package net.ucanaccess.test.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.DatabaseBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.console.Main;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessDriver;

public abstract class UcanaccessTestBase extends AbstractTestBase {

    protected static final AccessVersion DEFAULT_ACCESS_VERSION = AccessVersion.V2003;
    protected static final File          TEST_DB_TEMP_DIR       =
            new File(System.getProperty("java.io.tmpdir"), "ucanaccess-test");

    static {
        TEST_DB_TEMP_DIR.mkdirs();
        Main.setBatchMode(true);
    }

    private File                   fileAccDb;
    protected FileFormat           fileFormat;
    private String                 password          = "";
    protected UcanaccessConnection ucanaccess;
    private String                 user              = "ucanaccess";
    protected Connection           verifyConnection;
    private Boolean                ignoreCase;
    private long                   inactivityTimeout = -1;
    private String                 columnOrder;
    private String                 append2JdbcURL    = "";
    private Boolean                showSchema;

    @Parameterized.Parameters(name="{index}: {0}")
    public static Iterable<Object[]> getAllAccessFileFormats() {
        List<Object[]> fileFormats = Arrays.asList(
                new Object[] { FileFormat.V2000 },
                new Object[] { FileFormat.V2003 },
                new Object[] { FileFormat.V2007 },
                new Object[] { FileFormat.V2010 });
        return fileFormats;
    }

    public UcanaccessTestBase(AccessVersion _accessVersion) {
        this(_accessVersion.getFileFormat());
    }

    UcanaccessTestBase(FileFormat _fileFormat) {
        fileFormat = _fileFormat;
    }

    protected void setShowSchema(Boolean _showSchema) {
        showSchema = _showSchema;
    }

    protected Boolean getShowSchema() {
        return showSchema;
    }

    protected final File getFileAccDb() {
        return fileAccDb;
    }

    public void checkQuery(String _query, Object[][] _expected) throws SQLException, IOException {
        Statement st = null;
        ResultSet rs = null;
        try {
            st = ucanaccess.createStatement();
            rs = st.executeQuery(_query);
            diff(rs, _expected, _query);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
        }
    }

    public void checkQuery(String _query) throws SQLException, IOException {
        Statement st1 = null;
        ResultSet firstResultSet = null;
        Statement st2 = null;
        ResultSet verifyResultSet = null;
        try {
            initVerifyConnection();
            st1 = ucanaccess.createStatement();
            firstResultSet = st1.executeQuery(_query);
            st2 = verifyConnection.createStatement();
            verifyResultSet = st2.executeQuery(_query);
            diffResultSets(firstResultSet, verifyResultSet, _query);
        } finally {
            if (firstResultSet != null) {
                firstResultSet.close();
            }
            if (st1 != null) {
                st1.close();
            }
            if (verifyResultSet != null) {
                verifyResultSet.close();
            }
            if (st2 != null) {
                st2.close();
            }
            if (verifyConnection != null) {
                verifyConnection.close();
                verifyConnection = null;
            }
        }
    }

    public void checkQuery(String _query, Object... _expected) throws SQLException, IOException {
        checkQuery(_query, new Object[][] { _expected });
    }

    private void diff(ResultSet _resultSet, Object[][] _expectedResults, String _expression) throws SQLException, IOException {
        ResultSetMetaData mymeta = _resultSet.getMetaData();
        int mycolmax = mymeta.getColumnCount();
        if (_expectedResults.length > 0) {
            assertEquals(mycolmax, _expectedResults[0].length);
        }
        int j = 0;
        while (_resultSet.next()) {
            for (int i = 0; i < mycolmax; ++i) {
                assertTrue("Matrix with different length was expected: " + _expectedResults.length + " not" + j,
                        j < _expectedResults.length);
                Object actualObj = _resultSet.getObject(i + 1);
                Object expectedObj = _expectedResults[j][i];
                if (expectedObj == null) {
                    assertNull(actualObj);
                } else {
                    if (actualObj instanceof Blob) {
                        Blob blob = (Blob) actualObj;
                        InputStream bs = blob.getBinaryStream();
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        byte[] bt = new byte[4096];
                        int len;
                        while ((len = bs.read(bt)) != -1) {
                            bos.write(bt, 0, len);
                        }
                        bt = bos.toByteArray();
                        byte[] btMtx = (byte[]) expectedObj;
                        for (int y = 0; y < btMtx.length; y++) {
                            assertEquals(btMtx[y], bt[y]);
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
                        assertEquals("Expected ob2 and ob1 to be equal in [" + _expression + "]", expectedObj, actualObj);
                    }
                }
            }
            j++;
        }
        assertEquals("matrix with different length was expected ", _expectedResults.length, j);
    }

    public void diffResultSets(ResultSet _resultSet, ResultSet _verifyResultSet, String _query) throws SQLException, IOException {
        ResultSetMetaData mymeta = _resultSet.getMetaData();
        int mycolmax = mymeta.getColumnCount();
        ResultSetMetaData jometa = _verifyResultSet.getMetaData();
        int jocolmax = jometa.getColumnCount();
        assertTrue(jocolmax == mycolmax);
        StringBuffer log = new StringBuffer("{");
        int row = 0;
        while (next(_verifyResultSet, _resultSet)) {
            row++;
            if (log.length() > 1) {
                log.append(",");
            }
            log.append("{");
            for (int i = 0; i < mycolmax; ++i) {
                if (i > 0) {
                    log.append(",");
                }
                Object ob1 = _resultSet.getMetaData().getColumnType(i + 1) == Types.BOOLEAN ? _resultSet.getBoolean(i + 1) : _resultSet.getObject(i + 1);
                Object ob2 = _verifyResultSet.getMetaData().getColumnType(i + 1) == Types.BOOLEAN ? _verifyResultSet.getBoolean(i + 1) : _verifyResultSet.getObject(i + 1);

                if (ob1 == null && ob2 == null) {
                    // both null, ok
                } else if (ob1 == null) {
                    assertTrue("Object in verify set at row:col " + row + ":" + (i + 1) + " should be null, but was: " + ob2 + " in [" + _query + "]", ob2 == null);
                } else {
                    if (ob1 instanceof Blob) {
                        Blob blob = (Blob) ob1;
                        InputStream bs = blob.getBinaryStream();
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        byte[] bt = new byte[4096];
                        int len;
                        while ((len = bs.read(bt)) != -1) {
                            bos.write(bt, 0, len);
                        }
                        bt = bos.toByteArray();
                        byte[] btodbc = (byte[]) ob2;
                        for (int y = 0; y < btodbc.length; y++) {
                            assertEquals(btodbc[y], bt[y]);
                        }
                    } else if (ob1 instanceof ComplexBase[] && ob2 instanceof ComplexBase[]) {
                        assertTrue(Arrays.equals((ComplexBase[]) ob1, (ComplexBase[]) ob2));
                    }

                    else {
                        if (ob1 instanceof Number && ob2 instanceof Number) {
                            BigDecimal ob1b = new BigDecimal(ob1.toString());
                            BigDecimal ob2b = new BigDecimal(ob2.toString());
                            ob1 = ob1b.doubleValue();
                            ob2 = ob2b.doubleValue();
                        }
                        if (ob1 instanceof Date && ob2 instanceof Date) {
                            ob1 = ((Date) ob1).getTime();
                            ob2 = ((Date) ob2).getTime();
                        }
                        assertEquals(ob1, ob2);
                    }
                }
            }
            log.append("}");
        }
        log.append("}");

    }

    public void dumpQueryResult(ResultSet _resultSet) throws SQLException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true);
        new Main(ucanaccess, null).consoleDump(_resultSet, ps);
        String dumped = new String(baos.toByteArray());
        ps.close();
        getLogger().info(dumped);
    }

    public void dumpQueryResult(String _query) throws SQLException, IOException {
        Statement st = null;
        ResultSet resultSet = null;
        try {
            Connection conn = ucanaccess;
            st = conn.createStatement();
            resultSet = st.executeQuery(_query);
            dumpQueryResult(resultSet);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (st != null) {
                st.close();
            }
        }
    }

    public void dumpVerify(String expression) throws SQLException, IOException {
        Statement st = null;
        ResultSet myRs = null;
        try {
            Connection conn = verifyConnection;
            st = conn.createStatement();
            myRs = st.executeQuery(expression);
            dumpQueryResult(myRs);
        } finally {
            if (myRs != null) {
                myRs.close();
            }
            if (st != null) {
                st.close();
            }
        }
    }

    /**
     * Subclasses may provide their own test database by overriding this method.
     * @return valid classpath reference to a test database
     */
    public String getAccessPath() {
        return null;
    }

    public String getAccessTempPath() throws IOException {
        if (getAccessPath() == null) {
            fileAccDb = File.createTempFile(getClass().getSimpleName() + "-", fileFormat.getFileExtension(), TEST_DB_TEMP_DIR);
            createNewDatabase(fileFormat, fileAccDb);
        } else {
            fileAccDb = copyResourceToTempFile(getAccessPath());
            if (fileAccDb == null) {
                fileAccDb = new File(TEST_DB_TEMP_DIR, getAccessPath());
                if (!fileAccDb.exists()) {
                    createNewDatabase(fileFormat, fileAccDb);
                    fileAccDb.deleteOnExit();
                }
            }
        }

        return fileAccDb.getAbsolutePath();
    }

    void createNewDatabase(FileFormat _fileFormat, File _dbFile) throws IOException {
        Database db = DatabaseBuilder.create(_fileFormat, _dbFile);
        db.flush();
        db.close();
        getLogger().info("Access file version {} created: {}", _fileFormat.name(), _dbFile.getAbsolutePath());
    }

    protected File copyResourceToTempFile(String _resourcePath) throws IOException {
        File resourceFile = new File(_resourcePath);
        InputStream is = getClass().getClassLoader().getResourceAsStream(_resourcePath);
        if (is == null) {
            return null;
        }
        byte[] buffer = new byte[4096];
        File tempFile = File.createTempFile(resourceFile.getName().replace(".", "_"), fileFormat.getFileExtension(), TEST_DB_TEMP_DIR);
        getLogger().info("Copying resource file: {} to: {}", _resourcePath, tempFile.getAbsolutePath());
        FileOutputStream fos = new FileOutputStream(tempFile);
        int bread;
        while ((bread = is.read(buffer)) != -1) {
            fos.write(buffer, 0, bread);
        }
        fos.flush();
        fos.close();
        is.close();
        return tempFile;
    }

    public int getCount(String sql) throws SQLException, IOException {
        return this.getCount(sql, true);
    }

    public int getCount(String sql, boolean equals) throws SQLException, IOException {
        initVerifyConnection();
        Statement st = verifyConnection.createStatement();
        ResultSet joRs = st.executeQuery(sql);
        joRs.next();
        int count = joRs.getInt(1);
        st = ucanaccess.createStatement();
        ResultSet myRs = st.executeQuery(sql);
        myRs.next();
        int myCount = myRs.getInt(1);
        if (equals) {
            assertEquals(count, myCount);
        } else {
            assertFalse(count == myCount);
        }
        return count;
    }

    public String getName() {
        return getClass().getSimpleName() + " ver " + this.fileFormat;
    }

    protected String getPassword() {
        return password;
    }

    protected UcanaccessConnection getUcanaccessConnection() throws SQLException, IOException {
        return getUcanaccessConnection(UcanaccessDriver.URL_PREFIX, getAccessTempPath());
    }

    protected UcanaccessConnection getUcanaccessConnection(String _dbPath) throws SQLException, IOException {
        return getUcanaccessConnection(UcanaccessDriver.URL_PREFIX, _dbPath);
    }

    UcanaccessConnection getUcanaccessConnection(String _urlPrefix, String _dbPath) throws SQLException, IOException {
        if (_dbPath == null) {
            _dbPath = getAccessTempPath();
        }
        String url = _urlPrefix + _dbPath;
        if (this.ignoreCase != null) {
            url += ";ignoreCase=" + this.ignoreCase;
        }
        if (this.inactivityTimeout != -1) {
            url += ";inactivityTimeout=" + this.inactivityTimeout;
        } else {
            url += ";immediatelyreleaseresources=true";
        }
        if (this.columnOrder != null) {
            url += ";columnOrder=" + this.columnOrder;
        }
        if (this.showSchema != null) {
            url += ";showSchema=" + this.showSchema;
        }
        url += append2JdbcURL;
        return (UcanaccessConnection) DriverManager.getConnection(url, this.user, this.password);
    }

    protected void appendToJdbcURL(String s) {
        append2JdbcURL += s;
    }

    protected void initVerifyConnection() throws SQLException, IOException {
        InputStream is = new FileInputStream(fileAccDb);
        File tempVerifyFile = File.createTempFile(fileAccDb.getName().replace(".", "_") + "_verify", fileFormat.getFileExtension(), TEST_DB_TEMP_DIR);
        FileOutputStream fos = new FileOutputStream(tempVerifyFile);

        byte[] buffer = new byte[4096];
        int bread;
        while ((bread = is.read(buffer)) != -1) {
            fos.write(buffer, 0, bread);
        }
        fos.flush();
        fos.close();
        is.close();
        if (verifyConnection != null && !verifyConnection.isClosed()) {
            verifyConnection.close();
        }
        verifyConnection = getUcanaccessConnection(UcanaccessDriver.URL_PREFIX, tempVerifyFile.getAbsolutePath());
    }

    private boolean next(ResultSet _joRs, ResultSet _myRs) throws SQLException {
        boolean b1 = _joRs.next();
        boolean b2 = _myRs.next();
        assertEquals(b1, b2);
        return b1;
    }

    public void setInactivityTimeout(long _inactivityTimeout) {
        this.inactivityTimeout = _inactivityTimeout;
    }

    protected void setPassword(String _password) {
        this.password = _password;
    }

    protected void setColumnOrder(String _columnOrder) {
        this.columnOrder = _columnOrder;
    }

    public void setIgnoreCase(boolean _ignoreCase) {
        this.ignoreCase = _ignoreCase;

    }

    protected void dropTable(String _tableName) throws SQLException {
        executeStatements("DROP TABLE " + _tableName);
    }

    protected void executeStatements(String... _sqls) throws SQLException {
        Statement st = ucanaccess.createStatement();
        for (String sql : _sqls) {
            st.execute(sql);
        }
        st.close();
    }

    /**
     * Execute the specified sql on the given statement logging the root cause of an exception encountered.
     */
    protected void executeStatement(Statement _statement, String _sql) throws SQLException {
        try {
            _statement.execute(_sql);
        } catch (SQLException _ex) {
            getLogger().warn("Exception executing [" + _sql + "].", _ex.getCause() != null ? _ex.getCause() : _ex);
        }
    }

    @Before
    public final void beforeTestCaseBase() throws Exception {
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        ucanaccess = getUcanaccessConnection();
    }

    @After
    public final void afterTestCaseBase() throws Exception {
        if (ucanaccess != null && !ucanaccess.isClosed()) {
            try {
                ucanaccess.close();
            } catch (Exception _ex) {
                getLogger().warn("Database {} already closed: {}", fileAccDb, _ex);
            }
        }

        if (verifyConnection != null && !verifyConnection.isClosed()) {
            try {
                verifyConnection.close();
            } catch (Exception _ex) {
                getLogger().warn("Verify connection {} already closed: {}", verifyConnection, _ex);
            }
        }
    }

}
