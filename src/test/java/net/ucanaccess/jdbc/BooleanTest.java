package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;

class BooleanTest extends UcanaccessBaseTest {

    BooleanTest() {
        Locale.setDefault(Locale.US);
    }

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "boolean.accdb";
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements(
            "CREATE TABLE tblMain (id INT NOT NULL PRIMARY KEY, company TEXT NOT NULL, closed YESNO)",
            "INSERT INTO tblMain (id, company) VALUES(1, 'pippo')",
            "UPDATE tblMain SET closed=yes",
            "INSERT INTO t (pk) VALUES('pippo')");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        executeStatements("DROP TABLE tblMain");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCreate(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM tblMain");
        dumpQueryResult("SELECT * FROM t");
        checkQuery("SELECT * FROM tblMain");
        checkQuery("SELECT * FROM t");
    }

}
