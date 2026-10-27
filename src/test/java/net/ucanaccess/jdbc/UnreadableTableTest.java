package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.spannm.jackcess.*;
import io.github.spannm.jackcess.impl.DatabaseImpl;
import io.github.spannm.jackcess.impl.JetFormat;
import io.github.spannm.jackcess.impl.RowIdImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class UnreadableTableTest {

    @TempDir
    private File tempDir;

    @Test
    void connect_tableWithUnreadableRow_skipsTableAndKeepsOthers() throws Exception {
        File dbFile = new File(tempDir, "unreadable.accdb");
        createDatabase(dbFile);
        corruptFirstRow(dbFile, "Broken");

        try (UcanaccessConnection conn = new UcanaccessConnectionBuilder()
                .withDbPath(dbFile)
                .withoutUserPass()
                .withImmediatelyReleaseResources()
                .build()) {

            assertThat(warningMessages(conn.getWarnings())).contains("Cannot resolve table Broken");

            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT txt FROM Good")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo("fine");
            }
        }
    }

    /**
     * Creates a database with two tables of one row each.
     */
    private static void createDatabase(File dbFile) throws IOException {
        try (Database db = DatabaseBuilder.create(Database.FileFormat.V2010, dbFile)) {
            for (String name : List.of("Broken", "Good")) {
                Table table = new TableBuilder(name)
                    .addColumn(new ColumnBuilder("id", DataType.LONG))
                    .addColumn(new ColumnBuilder("txt", DataType.TEXT))
                    .toTable(db);
                table.addRow(1, "fine");
            }
        }
    }

    /**
     * Overwrites the variable length column offsets of the first row of the given table, so that reading it fails.
     */
    private static void corruptFirstRow(File dbFile, String tableName) throws IOException {
        int pageSize;
        RowIdImpl rowId;
        try (Database db = DatabaseBuilder.open(dbFile)) {
            JetFormat format = ((DatabaseImpl) db).getFormat();
            pageSize = format.PAGE_SIZE;
            rowId = (RowIdImpl) db.getTable(tableName).iterator().next().getId();
        }
        assertThat(rowId.getRowNumber()).isZero();

        // the first row of a data page ends at the end of the page, preceded by the null mask (1 byte for two
        // columns), the variable column count and the variable column offsets
        long rowEnd = (long) (rowId.getPageNumber() + 1) * pageSize;
        try (RandomAccessFile raf = new RandomAccessFile(dbFile, "rw")) {
            raf.seek(rowEnd - 7);
            raf.write(new byte[] {0, 0, (byte) 0xff, 0x0f});
        }
    }

    /**
     * Collects the messages of a warning chain.
     */
    private static List<String> warningMessages(SQLWarning warning) {
        List<String> messages = new ArrayList<>();
        for (SQLWarning w = warning; w != null; w = w.getNextWarning()) {
            messages.add(w.getMessage());
        }
        return messages;
    }

}
