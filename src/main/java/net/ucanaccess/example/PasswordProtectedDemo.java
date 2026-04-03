package net.ucanaccess.example;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Demo for accessing a password-protected MS Access (.accdb) file.
 * <p>
 * This class demonstrates the connection setup using a custom Jackcess opener,
 * retrieves database schema information via JDBC metadata, and prints table content.
 */
public final class PasswordProtectedDemo {

    private static final Logger LOG = LoggingConfigurator.configure(PasswordProtectedDemo.class.getName());

    private PasswordProtectedDemo() {
        throw new UnsupportedOperationException(
            "Utility class " + getClass().getSimpleName() + " cannot be instantiated");
    }

    /**
     * Executes the demo process: connecting, verifying metadata, and listing table rows.
     * <p>
     * The connection is established using the {@link PasswordProtectedOpener} specified in the JDBC URL.
     *
     * @param args command line arguments (not used)
     */
    public static void main(String[] args) {

        Path dbPath = Path.of("src/main/resources/password-protected.accdb");
        String password = "alligator3";
        String tableName = "t_demo";

        String url = String.format("jdbc:ucanaccess://%s;jackcessOpener=%s",
            dbPath, PasswordProtectedOpener.class.getName());

        LOG.log(Level.INFO, "About to connect to database ''{0}''", dbPath.getFileName());

        try (Connection conn = DriverManager.getConnection(url, "user", password)) {
            LOG.log(Level.INFO, "Connection to ''{0}'' successful", dbPath.getFileName());

            DatabaseMetaData dbMeta = conn.getMetaData();
            try (ResultSet rs = dbMeta.getTables(null, null, tableName, new String[] {"TABLE"})) {
                if (rs.next()) {
                    LOG.log(Level.INFO, "Verified existence of table ''{0}''", tableName);
                }
            }

            // query and print content of t_demo
            String sql = "SELECT * FROM " + tableName;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {

                ResultSetMetaData rsMeta = rs.getMetaData();
                int columnCount = rsMeta.getColumnCount();

                LOG.log(Level.INFO, "Printing content of table ''{0}'':", tableName);

                int rowCounter = 0;
                while (rs.next()) {
                    rowCounter++;
                    StringBuilder rowString = new StringBuilder("Row ").append(rowCounter).append(": [");
                    for (int i = 1; i <= columnCount; i++) {
                        rowString.append(rsMeta.getColumnName(i))
                                 .append('=')
                                 .append(rs.getObject(i));
                        if (i < columnCount) {
                            rowString.append(", ");
                        }
                    }
                    rowString.append(']');
                    LOG.info(rowString.toString());
                }
            }

            // clean shutdown to prevent lingering MemoryTimer threads
            try (Statement st = conn.createStatement()) {
                st.execute("SHUTDOWN");
            }

        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "Database operation failed", ex);
        } finally {
            LOG.info("Exiting application");
            System.exit(0);
        }
    }

}
