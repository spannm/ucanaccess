package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

class FloatTest extends UcanaccessBaseTest {

    FloatTest() {
        Locale.setDefault(Locale.US);
    }

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "float.accdb"; // Access 2007
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testCreate(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        checkQuery("SELECT [row] FROM t_float ORDER BY pk");
        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_float (row) values(?)");
        ps.setFloat(1, 1.4f);
        ps.execute();
        checkQuery("SELECT [row] FROM t_float ORDER BY pk");
        ps = ucanaccess.prepareStatement("UPDATE t_float SET [row]=?");
        ps.setObject(1, 4.9f);
        ps.execute();
        checkQuery("SELECT [row] FROM t_float ORDER BY pk");
        ps.setDouble(1, 0.0000000000000000000000000000000000000000000000001d);
        ps.execute();
        dumpQueryResult("SELECT * FROM t_float ORDER BY pk");
        checkQuery("SELECT [row] FROM t_float ORDER BY pk");
        ps.setFloat(1, 4.10011001155f);
        ps.execute();
        checkQuery("SELECT [row] FROM t_float ORDER BY pk");
        checkQuery("SELECT COUNT(*) FROM t_float WHERE [row]=4.10011", singleRec(2));
        dumpQueryResult("SELECT * FROM t_float ORDER BY pk");
    }

}
