package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Arrays;
import java.util.StringJoiner;

/**
 * Test to reproduce the HSQLDB cache size limit issue.
 */
class UCanAccessCacheReproductionTest extends UcanaccessBaseTest {

    private static final String  TABLE_NAME        = "StressTable";
    private static final int     COLUMN_COUNT      = 254;   // avoid 2GB MS Access size limit
    private static final int     COLUMN_SIZE       = 16000;
    private static final int     ROW_COUNT         = 15;
    private static final int     HSQLDB_CACHE_SIZE = 10000;
    private static final int     HSQLDB_CACHE_ROWS = 10;
    private static final boolean DELETE_ON_EXIT    = false;

    private final Path           dbPath            = createTempFileName(getClass().getSimpleName(), ".accdb").toPath();

    @Test
    void reproduceCacheSizeLimit() throws Exception {
        // Using memory=false and a very small cache size to force HSQLDB paging
        String url = String.format("jdbc:ucanaccess://%s;memory=false;hsqldbCacheSize=" + HSQLDB_CACHE_SIZE + ";hsqldbCacheRows=" + HSQLDB_CACHE_ROWS, dbPath);

        try {
            createEmptyAccessDatabase(dbPath);

            try (Connection conn = DriverManager.getConnection(url)) {
                try (Statement st = conn.createStatement()) {
                    st.execute("SET DATABASE DEFAULT TABLE TYPE CACHED");
                    // st.execute("SET FILES CACHE SIZE " + hsqldbCacheSize);
                }

                conn.setAutoCommit(false);

                createLargeTable(conn);
                executeMassInsert(conn);

                getLogger().log(Level.INFO, "Committing transaction ...");

                conn.commit();
                getLogger().log(Level.INFO, "Transaction committed successfully");
            }
        } catch (SQLException _ex) {
            getLogger().log(Level.ERROR, "Reproduction successful: {0}", _ex.getMessage());
            throw _ex;
        } finally {

            if (DELETE_ON_EXIT) {
                Files.deleteIfExists(dbPath);
            }

        }

        verifyData(url);
    }

    private void verifyData(String url) throws SQLException {
        getLogger().log(Level.INFO, "Re-opening database to verify data...");
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", TABLE_NAME))) {
            if (rs.next()) {
                getLogger().log(Level.INFO, "Verification successful, row count: {0}", rs.getInt(1));
            }
        }
    }

    /**
     * Creates an empty MS Access database file.
     * <p>
     * Uses Jackcess to create a V2019 format file.
     *
     * @param _dbFile the path to the database file
     * @throws IOException if file creation fails
     */
    void createEmptyAccessDatabase(final Path _dbFile) throws IOException {
        if (Files.exists(_dbFile)) {
            Files.delete(_dbFile);
        }
        try (Database db = DatabaseBuilder.create(Database.FileFormat.V2019, _dbFile.toFile())) {
            getLogger().log(Level.INFO, "Access database created at {0}", _dbFile.toAbsolutePath());
        }
    }

    void createLargeTable(final Connection _conn) throws SQLException {
        final StringJoiner columns = new StringJoiner(", ", "(id COUNTER PRIMARY KEY, ", ")");
        for (int i = 1; i <= COLUMN_COUNT; i++) {
            // VARCHAR wird in HSQLDB als VARCHAR gespeichert, nicht als CLOB
            // Limit muss unter 32KB bleiben um CLOB zu vermeiden
            columns.add(String.format("col%d VARBINARY(" + COLUMN_SIZE + ")", i));
        }

        final String sql = String.format("CREATE TABLE %s %s", TABLE_NAME, columns);

        try (Statement stmt = _conn.createStatement()) {
            stmt.execute(sql);
            // stmt.execute(String.format("CREATE INDEX idx_col1 ON %s (col1)", TABLE_NAME));
            getLogger().log(Level.INFO, "Table {0} created with {1} columns", TABLE_NAME, COLUMN_COUNT);
        }
    }

    void executeMassInsert(final Connection _conn) throws SQLException {
        final StringJoiner colNames = new StringJoiner(", ");
        final StringJoiner placeholders = new StringJoiner(", ");
        for (int i = 1; i <= COLUMN_COUNT; i++) {
            colNames.add(String.format("col%d", i));
            placeholders.add("?");
        }

        final String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", TABLE_NAME, colNames, placeholders);

        try (PreparedStatement pstmt = _conn.prepareStatement(sql)) {
            byte[] colVal = new byte[16000];
            Arrays.fill(colVal, (byte) 0xFF);
            // final String colVal = "A".repeat(COLUMN_SIZE);

            for (int r = 1; r <= ROW_COUNT; r++) {
                for (int c = 1; c <= COLUMN_COUNT; c++) {
                    // pstmt.setString(c, colVal);
                    pstmt.setBytes(c, colVal);
                }
                pstmt.addBatch();

                if (r % 1000 == 0) {
                    getLogger().log(Level.INFO, "Batching row {0}", r);
                }
            }

            getLogger().log(Level.INFO, "Executing batch insert...");
            pstmt.executeBatch();
        }
    }

}
