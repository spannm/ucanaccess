package net.ucanaccess.jackcess;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Utility class to inspect Microsoft Access database files using the Jackcess library.
 * <p>
 * This tool iterates through a list of database file paths provided via command-line arguments,
 * opens each valid database, and prints its file format along with a summary of all contained tables.
 * <p>
 * For each table found, it outputs the table name and the total row count. If a table cannot
 * be loaded or an error occurs during inspection, an error message is printed to the standard error stream.
 */
public class JackcessPrintTables {

    public static void main(String[] args) throws IOException {
        List<Path> dbFiles = Arrays.stream(args)
            .filter(Objects::nonNull)
            .map(Paths::get)
            .filter(Files::exists)
            .filter(Files::isReadable)
            .collect(Collectors.toList());

        if (dbFiles.isEmpty()) {
            System.err.println("No readable database files provided in arguments");
            return;
        }

        for (Path dbFile : dbFiles) {
            try (Database db = DatabaseBuilder.open(dbFile)) {
                System.out.printf("Database format of '%s': %s%n", dbFile.getFileName(), db.getFileFormat());

                for (String tableName : db.getTableNames()) {
                    try {
                        Table table = db.getTable(tableName);
                        Optional.ofNullable(table)
                            .ifPresentOrElse(
                                t -> System.out.printf("Table: %-30s | Rows: %d%n", tableName, t.getRowCount()),
                                () -> System.out.printf("Table: %-30s | Rows: 0 (Table not found)%n", tableName)
                            );
                    } catch (Exception ex) {
                        System.err.printf("Table: %-30s | Failed to load: %s%n", tableName, ex.getMessage());
                    }
                }
            }
        }
    }

}
