package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.*;

class BlobOleTest extends UcanaccessBaseTest {

    private static final String IMG_FILE_NAME  = "BlobOleTest/elisaArt.jpeg";
    private static final String PPTX_FILE_NAME = "BlobOleTest/test.pptx";

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_ole_test (id COUNTER PRIMARY KEY, c_descr TEXT(400), c_ole OLE)");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable("t_ole_test");
    }

    // It only works with JRE 1.6 and later (JDBC 3)
    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBlobOle(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        File imgFileTemp = createTempFile(IMG_FILE_NAME);
        
        Blob blob = ucanaccess.createBlob();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(IMG_FILE_NAME);
            OutputStream out = blob.setBinaryStream(1)) {
            is.transferTo(out);
        }

        String descr = "TestOle";
        try (PreparedStatement ps1 = ucanaccess.prepareStatement("INSERT INTO t_ole_test (c_descr, c_ole) VALUES(?, ?)")) {
            ps1.setString(1, descr);
            ps1.setBlob(2, blob);
            ps1.execute();
        }

        Statement st = ucanaccess.createStatement();
        ResultSet rs1 = st.executeQuery("SELECT c_ole FROM t_ole_test");
        rs1.next();

        try (InputStream isFromDb = rs1.getBinaryStream(1)) {
            
            Files.copy(isFromDb, imgFileTemp.toPath(), StandardCopyOption.REPLACE_EXISTING);
            getLogger().info("Image file was created in {}", imgFileTemp.getAbsolutePath());
            
            byte[] fileBytes = Files.readAllBytes(imgFileTemp.toPath());

            checkQuery("SELECT * FROM t_ole_test", new Object[][] {{1, descr, fileBytes}});

            PreparedStatement ps2 = ucanaccess.prepareStatement("UPDATE t_ole_test SET c_descr=? WHERE c_descr=?");
            ps2.setString(1, descr + "_OK");
            ps2.setString(2, descr);
            ps2.executeUpdate();
            checkQuery("SELECT * FROM t_ole_test");
            checkQuery("SELECT * FROM t_ole_test", 1, descr + "_OK", fileBytes);
        }

        try (PreparedStatement ps3 = ucanaccess.prepareStatement("UPDATE t_ole_test SET c_ole=? WHERE c_descr=?")) {
            File fl1 = copyFileFromClasspath(PPTX_FILE_NAME);
            blob = ucanaccess.createBlob(fl1);
            ps3.setObject(1, fl1);
            ps3.setString(2, descr + "_OK");
            ps3.executeUpdate();
            fl1.delete();
            getLogger().info("PPTX file was created in {}", getFileAccDb());
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

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBlobPackaged(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        PreparedStatement ps = null;
        File fl1 = copyFileFromClasspath(PPTX_FILE_NAME);
        Blob blob = ucanaccess.createBlob(fl1);
        ps = ucanaccess.prepareStatement("INSERT INTO t_ole_test (c_descr, c_ole)  VALUES( ?,?)");
        ps.setString(1, "TestOle");
        ps.setBlob(2, blob);

        ps.execute();
        getLogger().info("PPTX file was created in {}", getFileAccDb());
        checkQuery("SELECT * FROM t_ole_test");
        Statement st = ucanaccess.createStatement();
        fl1.delete();
        st.execute("DELETE FROM t_ole_test");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testTwoColumnPk(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        // fix for ticket #23 should prevent this test from throwing an error
        try (Statement st = ucanaccess.createStatement()) {
            assertDoesNotThrow(() -> st.execute("CREATE TABLE t_two_col_pk (pk_col1 LONG, pk_col2 LONG, blob_col OLE, "
                + "CONSTRAINT pk_t_two_col_pk PRIMARY KEY (pk_col1, pk_col2))"));
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

    private File copyFileFromClasspath(String _fn) throws IOException {
        int idxLastDot = _fn.lastIndexOf('.');
        String prefix = idxLastDot < 0 ? _fn : _fn.substring(0, idxLastDot);
        String suffix = idxLastDot < 0 ? ".tmp" : _fn.substring(idxLastDot).toLowerCase();
        File tmpFile = createTempFileName(prefix, suffix);
        tmpFile.deleteOnExit();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(_fn)) {
            Files.copy(is, tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        return tmpFile;
    }

}
