package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

class ColumnOrderTest extends UcanaccessBaseFileTest {

    @Override
    protected UcanaccessConnectionBuilder buildConnection() {
        return super.buildConnection()
            .withColumnOrderDisplay();
    }

    @Test
    void testColumnOrder1() throws Exception {
        init();

        try (Connection uca = createUcanaccessConnection();
            PreparedStatement ps = uca.prepareStatement("INSERT INTO t_columnorder values (?, ?, ?)")) {
            ps.setInt(3, 3);
            ps.setDate(2, new Date(System.currentTimeMillis()));
            ps.setString(1, "This is the display order");
        }
    }
}
