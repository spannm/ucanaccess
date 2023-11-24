package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.SQLException;

class PivotTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "pivot.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testPivot(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            dumpQueryResult("SELECT * FROM Table1_trim");
            executeStatements(st,
                "INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('O SOLE',1234.56,#2003-12-03# )",
                "INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('O SOLE MIO',134.46,#2003-12-03# )",
                "INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('STA IN FRUNTE A MEEE',1344.46,#2003-12-05# )");
            initVerifyConnection();
            dumpQueryResult("SELECT * FROM Table1_trim");
            checkQuery("SELECT * FROM Table1_trim");
        }
    }
}
