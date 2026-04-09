package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.System.Logger.Level;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Tests for Date/Time precision handling during update operations.
 */
class DateTimeUpdatePersistTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource(include = "V2016")
    void testUpdateDateTimePersisted(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        // path to the physical file for the second connection
        String accdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();
        int id = 100;

        // Access Date/Time supports only millisecond precision.
        // we use full precision for createTime to trigger the internal truncation logic,
        // but we must compare against a truncated version.
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime createTime = now;
        LocalDateTime updateTime = now.plusSeconds(10);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE t_datetime_update (id INTEGER PRIMARY KEY, full_name TEXT(50), phone TEXT(20), create_time DATETIME, update_time DATETIME)");
        }

        String insertSql = "INSERT INTO t_datetime_update (id, full_name, create_time) VALUES (?, ?, ?)";
        try (PreparedStatement ps = ucanaccess.prepareStatement(insertSql)) {
            ps.setInt(1, id);
            ps.setString(2, "Jane Smith");
            ps.setObject(3, createTime);
            ps.executeUpdate();
        }

        // perform the update that is reportedly not persisted
        String updateSql = "UPDATE t_datetime_update SET phone = ?, update_time = ? WHERE id = ?";
        try (PreparedStatement ps = ucanaccess.prepareStatement(updateSql)) {
            ps.setString(1, "555-1111");
            ps.setObject(2, updateTime);
            ps.setInt(3, id);
            int updated = ps.executeUpdate();
            assertEquals(1, updated);
        }

        // close the first connection to force flushing and release file locks
        ucanaccess.close();

        // open a new connection to verify persistence in the physical file
        try (UcanaccessConnection conn = buildConnection()
                .withDbPath(accdbPath)
                .withImmediatelyReleaseResources()
                .build();
             PreparedStatement ps = conn.prepareStatement("SELECT phone, update_time FROM t_datetime_update WHERE id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();

                String actualPhone = rs.getString("phone");
                Timestamp actualUpdateTime = rs.getTimestamp("update_time");

                getLogger().log(Level.INFO, "Verified record: phone={0}, update_time={1}",
                    new Object[] {actualPhone, actualUpdateTime});

                assertEquals("555-1111", actualPhone);

                // compare using millisecond precision as Access/Jackcess truncates the rest
                assertEquals(Timestamp.valueOf(updateTime.truncatedTo(ChronoUnit.MILLIS)), actualUpdateTime);
            }
        }
    }

}

