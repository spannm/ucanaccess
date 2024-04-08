package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;

class FloatTest extends UcanaccessBaseFileTest {

    @BeforeAll
    static void setLocale() {
        locale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @AfterAll
    static void resetLocale() {
        Locale.setDefault(Objects.requireNonNullElseGet(locale, Locale::getDefault));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource(include = "V2007")
    void testCreate(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        checkQuery("SELECT [row] FROM t_float ORDER BY pk");
        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_float (row) VALUES(?)");
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
