package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;

class ByteTest extends UcanaccessBaseTest {

    ByteTest() {
        Locale.setDefault(Locale.US);
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements(
            "CREATE TABLE t_byte (ID int NOT NULL PRIMARY KEY, company TEXT NOT NULL, Closed byte); ",
            "INSERT INTO t_byte (id,company) VALUES(1, 'pippo')",
            "UPDATE t_byte SET closed=255");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCreate(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM t_byte");
        checkQuery("SELECT * FROM t_byte");
    }

}
