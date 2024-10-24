package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.System.Logger.Level;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

class BlobOleLazyLoadingTest extends UcanaccessBaseFileTest {

    // It only works with JRE 1.6 and later (JDBC 3)
    @Test
    void testBlobOLE() throws SQLException, IOException {
        init();

        final long binaryFileSize = 32718;
        byte[] initialBlobBytes = getBlobBytes();
        getLogger().log(Level.INFO, "BLOB size in backing database before retrieval: {0} bytes", initialBlobBytes.length);
        assertThat((long) initialBlobBytes.length).isLessThan(binaryFileSize);
        UcanaccessStatement st = ucanaccess.createStatement();
        ResultSet rs = st.executeQuery("SELECT Ole FROM OleTable ORDER BY ID");
        File file = createTempFileName("Copied", ".jpeg");
        rs.next();
        @SuppressWarnings("unused")
        Object obj = rs.getObject(1);
        try (InputStream isFromDb = rs.getBlob(1).getBinaryStream()) {
            copyFile(isFromDb, file).deleteOnExit();
        }
        assertEquals(file.length(), binaryFileSize);
        getLogger().log(Level.INFO, "File was created in {0}, size: {1} bytes", file.getAbsolutePath(), file.length());
        byte[] finalBlobBytes = getBlobBytes();
        getLogger().log(Level.INFO, "BLOB size in backing database after retrieval: {0} bytes", finalBlobBytes.length);
        if (!Arrays.equals(initialBlobBytes, finalBlobBytes)) {
            getLogger().log(Level.WARNING, "Simply retrieving BLOB changed byte data in backing database. Problem?");
        }
    }

    private byte[] getBlobBytes() throws SQLException {
        try (Statement hsqlSt = ucanaccess.getHSQLDBConnection().createStatement();
             ResultSet hsqlRs = hsqlSt.executeQuery("SELECT Ole FROM OleTable ORDER BY ID")) {
            hsqlRs.next();
            return hsqlRs.getBytes(1);
        }
    }

}
