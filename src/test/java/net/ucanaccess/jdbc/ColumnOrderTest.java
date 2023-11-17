package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

class ColumnOrderTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "columnOrder.accdb";
    }

    @Override
    protected UcanaccessConnectionBuilder buildConnection() {
        return super.buildConnection()
            .withColumnOrderDisplay();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testColumnOrder1(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (Connection uca = createUcanaccessConnection();
            PreparedStatement ps = uca.prepareStatement("INSERT INTO t1 values (?, ?, ?)")) {
            ps.setInt(3, 3);
            ps.setDate(2, new Date(System.currentTimeMillis()));
            ps.setString(1, "This is the display order");
        }
    }
}
