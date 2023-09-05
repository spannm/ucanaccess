package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

@RunWith(Parameterized.class)
public class ColumnOrderTest extends AccessVersionDefaultTest {

    public ColumnOrderTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/columnOrder.accdb";
    }

    @Test
    public void testColumnOrder1() throws Exception {
        setColumnOrder("display");
        Connection uca = getUcanaccessConnection();
        PreparedStatement ps = uca.prepareStatement("insert into t1 values (?,?,?)");
        ps.setInt(3, 3);
        ps.setDate(2, new Date(System.currentTimeMillis()));
        ps.setString(1, "This is the display order");
        ps.close();
        uca.close();
    }
}
