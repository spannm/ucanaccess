package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.sql.*;

class BlobOleTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_ole_test (id COUNTER PRIMARY KEY, c_descr TEXT(400), c_ole OLE)");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        executeStatements("DROP TABLE t_ole_test");
    }

    // It only works with JRE 1.6 and later (JDBC 3)
    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBlobOle(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        String imgFileName = String.join(File.separator, getClass().getSimpleName(), "blobOleTest.jpeg");

        Blob blob = ucanaccess.createBlob();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(imgFileName.replace(File.separatorChar, '/'));
            OutputStream out = blob.setBinaryStream(1)) {
            is.transferTo(out);
        }

        String descr = "TestOle";
        try (PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_ole_test (c_descr, c_ole) VALUES(?, ?)")) {
            ps.setString(1, descr);
            ps.setBlob(2, blob);
            ps.execute();
        }

        Statement st = ucanaccess.createStatement();
        ResultSet rs1 = st.executeQuery("SELECT c_ole FROM t_ole_test");
        rs1.next();

        try (InputStream isFromDb = rs1.getBinaryStream(1)) {

            File imgFileTemp = createTempFileName(imgFileName, null);
            copyFile(isFromDb, imgFileTemp).deleteOnExit();
            getLogger().info("Image file was created in {}", imgFileTemp.getAbsolutePath());

            byte[] fileBytes = Files.readAllBytes(imgFileTemp.toPath());

            checkQuery("SELECT * FROM t_ole_test", new Object[][] {{1, descr, fileBytes}});

            PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_ole_test SET c_descr=? WHERE c_descr=?");
            ps.setString(1, descr + "_OK");
            ps.setString(2, descr);
            ps.executeUpdate();
            checkQuery("SELECT * FROM t_ole_test");
            checkQuery("SELECT * FROM t_ole_test", 1, descr + "_OK", fileBytes);
        }

        try (PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_ole_test SET c_ole=? WHERE c_descr=?")) {
            String binFileName = String.join(File.separator, getClass().getSimpleName(), "blobOleTest.mp4");
            File file = createTempFileName(binFileName, null);
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(binFileName.replace('/', File.separatorChar))) {
                copyFile(is, file).deleteOnExit();
            }
            blob = ucanaccess.createBlob(file);
            ps.setObject(1, file);
            ps.setString(2, descr + "_OK");
            ps.executeUpdate();
            getLogger().info("Binary file was created in {}", getFileAccDb());
            checkQuery("SELECT * FROM t_ole_test");
        }

        try (PreparedStatement ps4 = ucanaccess.prepareStatement("DELETE FROM t_ole_test WHERE c_descr=?")) {
            ps4.setString(1, descr + "_OK");
            ps4.executeUpdate();
        }

        try (PreparedStatement ps5 = ucanaccess.prepareStatement("INSERT INTO t_ole_test (c_descr) VALUES (?)")) {
            ps5.setString(1, "value of OLE column is null");
            ps5.executeUpdate();
            try (ResultSet rs2 = st.executeQuery("SELECT c_ole FROM t_ole_test WHERE c_descr = 'value of OLE column is null'")) {
                rs2.next();
                assertNull(rs2.getBinaryStream(1));
                assertNull(rs2.getBlob(1));
            }
        }
    }

    // It only works with JRE 1.6 and later (JDBC 3)

    /**
     * @param _accessVersion
     * @throws SQLException
     * @throws IOException
     */
    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBlobPackaged(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        String binFileName = "BlobOleTest/blobOleTest.mp4";
        File file = createTempFileName(binFileName.replace('/', File.separatorChar), null);
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(binFileName)) {
            copyFile(is, file).deleteOnExit();
        }
        Blob blob = ucanaccess.createBlob(file);
        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_ole_test (c_descr, c_ole) VALUES( ?,?)");
        ps.setString(1, "TestOle");
        ps.setBlob(2, blob);

        ps.execute();
        getLogger().info("Binary file was created in {}", getFileAccDb());
        checkQuery("SELECT * FROM t_ole_test");
        Statement st = ucanaccess.createStatement();
        st.execute("DELETE FROM t_ole_test");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testTwoColumnPk(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        // fix for ticket #23 should prevent this test from throwing an error
        try (Statement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE t_two_col_pk (pk_col1 LONG, pk_col2 LONG, blob_col OLE, "
                + "CONSTRAINT pk_t_two_col_pk PRIMARY KEY (pk_col1, pk_col2))");
        }

        try (PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_two_col_pk VALUES (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 1);
            Blob b = ucanaccess.createBlob();
            b.setBytes(1, new byte[] {1});
            ps.setBlob(3, b);
            ps.executeUpdate();
        }
    }

}
