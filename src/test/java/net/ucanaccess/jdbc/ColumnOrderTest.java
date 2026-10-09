package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

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

        try (PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_columnorder values (?, ?, ?)")) {
            ps.setInt(3, 3);
            ps.setDate(2, new Date(System.currentTimeMillis()));
            ps.setString(1, "This is the display order");
            assertThat(ps.executeUpdate()).isOne();
        }
        checkQuery("SELECT txt3, ID1 FROM t_columnorder", singleRec("This is the display order", 3));
    }
}
