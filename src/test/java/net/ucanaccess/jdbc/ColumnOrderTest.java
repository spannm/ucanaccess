package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

class ColumnOrderTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "columnOrder.accdb";
    }

    @Override
    protected UcanaccessConnectionBuilder buildConnection() {
        return super.buildConnection()
            .withColumnOrderDisplay();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testColumnOrder1(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (Connection uca = createUcanaccessConnection();
            PreparedStatement ps = uca.prepareStatement("INSERT INTO t_columnorder values (?, ?, ?)")) {
            ps.setInt(3, 3);
            ps.setDate(2, new Date(System.currentTimeMillis()));
            ps.setString(1, "This is the display order");
        }
    }
}
