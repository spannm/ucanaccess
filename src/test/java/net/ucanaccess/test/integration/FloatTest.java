package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

class FloatTest extends UcanaccessTestBase {

    FloatTest() {
        Locale.setDefault(Locale.US);
    }

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "float.accdb"; // Access 2007
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testCreate(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

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
