package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2010Test;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class BlobOleLazyLoadingTest extends AccessVersion2010Test {

    public BlobOleLazyLoadingTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/LazyLoading.accdb";
    }

    // It only works with JRE 1.6 and later (JDBC 3)
    @Test
    public void testBlobOLE() throws SQLException, IOException {
        final long binaryFileSize = 32718;
        byte[] initialBlobBytes = getBlobBytes();
        getLogger().info("BLOB size in backing database before retrieval: {} bytes", initialBlobBytes.length);
        assertTrue(initialBlobBytes.length < binaryFileSize);
        Statement st = ucanaccess.createStatement();
        ResultSet rs = st.executeQuery("SELECT Ole FROM OleTable ORDER BY ID");
        File fl = new File(TEST_DB_TEMP_DIR + "/Copied.jpeg");
        rs.next();
        @SuppressWarnings("unused")
        Object obj = rs.getObject(1);
        InputStream isDB = rs.getBlob(1).getBinaryStream();
        OutputStream outFile = new FileOutputStream(fl);
        byte[] ba = new byte[4096];
        int len;
        while ((len = isDB.read(ba)) != -1) {
            outFile.write(ba, 0, len);
        }
        outFile.flush();
        outFile.close();
        assertEquals(fl.length(), binaryFileSize);
        getLogger().info("file was created in {}.", fl.getAbsolutePath());
        getLogger().info("file size {}.", fl.length());
        fl.delete();
        byte[] finalBlobBytes = getBlobBytes();
        getLogger().info("BLOB size in backing database after retrieval: {} bytes", finalBlobBytes.length);
        if (!Arrays.equals(initialBlobBytes, finalBlobBytes)) {
            getLogger().warn("Simply retrieving BLOB changed byte data in backing database. Problem?");
        }
    }

    private byte[] getBlobBytes() throws SQLException {
        Statement hsqlSt = ucanaccess.getHSQLDBConnection().createStatement();
        ResultSet hsqlRs = hsqlSt.executeQuery("SELECT OLE FROM OLETABLE ORDER BY ID");
        hsqlRs.next();
        byte[] blobBytes = hsqlRs.getBytes(1);
        hsqlRs.close();
        hsqlSt.close();
        return blobBytes;
    }
}
