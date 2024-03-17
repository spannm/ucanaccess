package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class BlobOleTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_ole_test (id COUNTER PRIMARY KEY, c_descr TEXT(400), c_ole OLE)");
    }

    // It only works with JRE 1.6 and later (JDBC 3)
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBlobOle(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        String imgFileName = getFileResource("blobOleTest.jpg"); // media file (c) Markus Spann

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

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT c_ole FROM t_ole_test");
            rs.next();

            try (InputStream isFromDb = rs.getBinaryStream(1)) {

                File imgFileTemp = createTempFileName(imgFileName, null);
                copyFile(isFromDb, imgFileTemp).deleteOnExit();
                getLogger().log(Level.INFO, "Image file was created in {0}", imgFileTemp.getAbsolutePath());

                byte[] fileBytes = Files.readAllBytes(imgFileTemp.toPath());

                checkQuery("SELECT * FROM t_ole_test", singleRec(1, descr, fileBytes));

                PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_ole_test SET c_descr=? WHERE c_descr=?");
                ps.setString(1, descr + "_OK");
                ps.setString(2, descr);
                ps.executeUpdate();
                checkQuery("SELECT * FROM t_ole_test");
                checkQuery("SELECT * FROM t_ole_test", singleRec(1, descr + "_OK", fileBytes));
            }
        }

        String binFileName = getFileResource("blobOleTest.gif"); // media file (c) Markus Spann

        try (PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_ole_test SET c_ole=? WHERE c_descr=?")) {
            File file = createTempFileName(binFileName, null);
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(binFileName.replace('/', File.separatorChar))) {
                copyFile(is, file).deleteOnExit();
            }
            blob = ucanaccess.createBlob(file);
            ps.setObject(1, file);
            ps.setString(2, descr + "_OK");
            ps.executeUpdate();
            getLogger().log(Level.INFO, "Binary file was created in {0}", getFileAccDb());
            checkQuery("SELECT * FROM t_ole_test");
        }

        try (PreparedStatement ps4 = ucanaccess.prepareStatement("DELETE FROM t_ole_test WHERE c_descr=?")) {
            ps4.setString(1, descr + "_OK");
            ps4.executeUpdate();
        }

        try (PreparedStatement ps5 = ucanaccess.prepareStatement("INSERT INTO t_ole_test (c_descr) VALUES (?)")) {
            ps5.setString(1, "value of OLE column is null");
            ps5.executeUpdate();
            try (UcanaccessStatement st = ucanaccess.createStatement();
                 ResultSet rs = st.executeQuery("SELECT c_ole FROM t_ole_test WHERE c_descr = 'value of OLE column is null'")) {
                rs.next();
                assertNull(rs.getBinaryStream(1));
                assertNull(rs.getBlob(1));
            }
        }
    }

    private String getFileResource(String fileName) {
        String folder = getClass().getSimpleName();
        folder = folder.substring(0, 1).toLowerCase() + folder.substring(1);
        return String.join(File.separator, folder, fileName);
    }

    // It only works with JRE 1.6 and later (JDBC 3)
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBlobPackaged(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        String binFileName = getFileResource("blobOleTest.gif"); // media file (c) Markus Spann
        File file = createTempFileName(binFileName.replace('/', File.separatorChar), null);

        try (InputStream is = getClass().getClassLoader().getResourceAsStream(binFileName)) {
            copyFile(is, file).deleteOnExit();
        }
        Blob blob = ucanaccess.createBlob(file);
        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_ole_test (c_descr, c_ole) VALUES( ?,?)");
        ps.setString(1, "TestOle");
        ps.setBlob(2, blob);

        ps.execute();
        getLogger().log(Level.INFO, "Binary file was created in {0}", getFileAccDb());
        checkQuery("SELECT * FROM t_ole_test");
        UcanaccessStatement st = ucanaccess.createStatement();
        st.execute("DELETE FROM t_ole_test");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testTwoColumnPk(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        // fix for ticket #23 should prevent this test from throwing an error
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
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
