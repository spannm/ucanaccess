package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.sql.*;

@RunWith(Parameterized.class)
public class BlobOleTest extends AccessVersionAllTest {

    private static final String IMG_FILE_NAME  = "elisaArt.JPG";
    private static final String PPTX_FILE_NAME = "test.pptx";

    public BlobOleTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE T2 (id COUNTER primary key, descr TEXT(400), pippo OLE)");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("T2");
    }

    // It only works with JRE 1.6 and later (JDBC 3)
    @Test
    public void testBlobOLE() throws SQLException, IOException {
        final File fl = new File("CopyElisaArt.JPG");

        Blob blob = ucanaccess.createBlob();
        OutputStream out = blob.setBinaryStream(1);
        InputStream is = getClass().getClassLoader().getResourceAsStream(IMG_FILE_NAME);
        byte[] ba = new byte[4096];
        int len;
        while ((len = is.read(ba)) != -1) {
            out.write(ba, 0, len);
        }
        out.flush();
        out.close();

        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO T2 (descr,pippo)  VALUES( ?,?)");
        ps.setString(1, "TestOle");
        ps.setBlob(2, blob);
        ps.execute();
        Statement st = ucanaccess.createStatement();
        ResultSet rs = st.executeQuery("SELECT Pippo FROM T2");
        rs.next();
        InputStream isDB = rs.getBinaryStream(1);
        OutputStream outFile = new FileOutputStream(fl);
        ByteArrayOutputStream outByte = new ByteArrayOutputStream();
        ba = new byte[4096];
        while ((len = isDB.read(ba)) != -1) {
            outFile.write(ba, 0, len);
            outByte.write(ba, 0, len);
        }
        outFile.flush();
        outFile.close();
        getLogger().info("Image file was created in {}.", fl.getAbsolutePath());
        outByte.flush();
        outByte.close();
        checkQuery("SELECT * FROM T2", new Object[][] {{1, "TestOle", outByte.toByteArray()}});
        ps.close();

        ps = ucanaccess.prepareStatement("UPDATE T2 SET descr=? WHERE  descr=?");
        ps.setString(1, "TestOleOk");
        ps.setString(2, "TestOle");
        ps.executeUpdate();
        checkQuery("SELECT * FROM T2");
        checkQuery("SELECT * FROM T2", 1, "TestOleOk", outByte.toByteArray());
        ps = ucanaccess.prepareStatement("UPDATE T2 SET pippo=? WHERE  descr=?");
        File fl1 = getFile(PPTX_FILE_NAME);
        blob = ucanaccess.createBlob(fl1);
        ps.setObject(1, fl1);
        ps.setString(2, "TestOleOk");
        ps.executeUpdate();

        getLogger().info("PPTX file was created in {}.", getFileAccDb());
        checkQuery("SELECT * FROM T2");
        ps = ucanaccess.prepareStatement("DELETE FROM  t2  WHERE  descr=?");
        ps.setString(1, "TestOleOk");
        ps.executeUpdate();

        ps.close();

        ps = ucanaccess.prepareStatement("INSERT INTO t2 (descr) VALUES (?)");
        ps.setString(1, "OLE column is null");
        ps.executeUpdate();
        rs = st.executeQuery("SELECT pippo FROM t2 WHERE descr='OLE column is null'");
        rs.next();
        assertNull(rs.getBinaryStream(1));
        assertNull(rs.getBlob(1));

        rs.close();
        ps.close();
        fl.delete();
        fl1.delete();
    }

    // It only works with JRE 1.6 and later (JDBC 3)

    @Test
    public void testBlobPackaged() throws SQLException, IOException {
        PreparedStatement ps = null;
        File fl1 = getFile(PPTX_FILE_NAME);
        Blob blob = ucanaccess.createBlob(fl1);
        ps = ucanaccess.prepareStatement("INSERT INTO T2 (descr,pippo)  VALUES( ?,?)");
        ps.setString(1, "TestOle");
        ps.setBlob(2, blob);

        ps.execute();
        getLogger().info("PPTX file was created in {}.", getFileAccDb());
        checkQuery("SELECT * FROM T2");
        Statement st = ucanaccess.createStatement();
        fl1.delete();
        st.execute("DELETE FROM t2");
    }

    @Test
    public void testTwoColumnPK() throws SQLException {
        // fix for ticket #23 should prevent this test from throwing an error
        Statement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE two_col_pk (pk_col1 LONG, pk_col2 LONG, blob_col OLE, "
            + "CONSTRAINT PK_two_col_pk PRIMARY KEY (pk_col1, pk_col2))");
        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO two_col_pk VALUES (?, ?, ?)");
        ps.setInt(1, 1);
        ps.setInt(2, 1);
        Blob b = ucanaccess.createBlob();
        b.setBytes(1, new byte[] {1});
        ps.setBlob(3, b);
        ps.executeUpdate();
    }

    private File getFile(String fn) throws IOException {
        // fix for ticket #23 should prevent this test f
        File fl1 = new File(fn);
        FileOutputStream out = new FileOutputStream(fl1);
        InputStream is = getClass().getClassLoader().getResourceAsStream(fn);
        byte[] ba = new byte[4096];
        int len;
        while ((len = is.read(ba)) != -1) {
            out.write(ba, 0, len);
        }
        out.flush();
        out.close();
        return fl1;
    }
}
