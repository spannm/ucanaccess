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
            "CREATE TABLE tblMain (ID int NOT NULL PRIMARY KEY,company TEXT NOT NULL, Closed byte); ",
            "INSERT INTO tblMain (id,company) VALUES(1, 'pippo')",
            "UPDATE tblMain SET closed=255");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCreate(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM tblMain");
        checkQuery("SELECT * FROM tblMain");
    }

}
