package net.ucanaccess.converters;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.spannm.jackcess.*;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessConnectionBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a SELECT without FROM on a database with a table named Dual, which must not be confused with the one row table
 * DUAL of HSQLDB.
 */
@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class DualTableTest {

    @TempDir
    private File tempDir;

    /**
     * Resets the flag set by loading the table Dual, which would otherwise rename DUAL in the following tests.
     */
    @AfterEach
    void tearDown() {
        SQLConverter.setDualUsedAsTableName(false);
    }

    @Test
    void executeQuery_tableNamedDual_selectWithoutFromReturnsOneRow() throws Exception {
        File dbFile = new File(tempDir, "dual.accdb");
        createDatabase(dbFile);

        try (UcanaccessConnection conn = new UcanaccessConnectionBuilder()
                .withDbPath(dbFile)
                .withoutUserPass()
                .withImmediatelyReleaseResources()
                .build()) {
            assertThat(query(conn, "SELECT 42")).containsExactly("42");
        }
    }

    /**
     * Creates a database with a table Dual of two rows.
     */
    private static void createDatabase(File dbFile) throws IOException {
        try (Database db = DatabaseBuilder.create(Database.FileFormat.V2010, dbFile)) {
            Table table = new TableBuilder("Dual")
                .addColumn(new ColumnBuilder("x", DataType.TEXT))
                .toTable(db);
            table.addRow("u1");
            table.addRow("u2");
        }
    }

    /**
     * Returns the first column of all rows of the given query as text.
     */
    private static List<String> query(UcanaccessConnection conn, String sql) throws Exception {
        List<String> values = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                values.add(rs.getString(1));
            }
        }
        return values;
    }
}
