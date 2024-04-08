package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;

class BooleanTest extends UcanaccessBaseFileTest {

    @BeforeAll
    static void setLocale() {
        locale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @AfterAll
    static void resetLocale() {
        Locale.setDefault(Objects.requireNonNullElseGet(locale, Locale::getDefault));
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

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCreate(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM tblMain");
        dumpQueryResult("SELECT * FROM t");
        checkQuery("SELECT * FROM tblMain");
        checkQuery("SELECT * FROM t");
    }

}
