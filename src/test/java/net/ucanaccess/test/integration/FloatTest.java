package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2007Test;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

@RunWith(Parameterized.class)
public class FloatTest extends AccessVersion2007Test {

    public FloatTest(AccessVersion _accessVersion) {
        super(_accessVersion);
        Locale.setDefault(Locale.US);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/float.accdb"; // Access 2007
    }

    @Test
    public void testCreate() throws SQLException, IOException {
        checkQuery("SELECT [row] FROM t ORDER BY pk");
        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t (row) values(?)");
        ps.setFloat(1, 1.4f);
        ps.execute();
        checkQuery("SELECT [row] FROM t ORDER BY pk");
        ps = ucanaccess.prepareStatement("update t  set [row]=?");
        ps.setObject(1, 4.9f);
        ps.execute();
        checkQuery("SELECT [row] FROM t ORDER BY pk");
        ps.setDouble(1, 0.0000000000000000000000000000000000000000000000001d);
        ps.execute();
        dumpQueryResult("SELECT * FROM t ORDER BY pk");
        checkQuery("SELECT [row] FROM t ORDER BY pk");
        ps.setFloat(1, 4.10011001155f);
        ps.execute();
        checkQuery("SELECT [row] FROM t ORDER BY pk");
        checkQuery("SELECT COUNT(*) FROM t WHERE [row]=4.10011", 2);
        dumpQueryResult("SELECT * FROM t ORDER BY pk");

    }

}
