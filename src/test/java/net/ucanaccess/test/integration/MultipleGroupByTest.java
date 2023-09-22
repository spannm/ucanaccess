package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.SQLException;

class MultipleGroupByTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testMultiple(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        Connection conn = getUcanaccessConnection();
        String wCreateTable = "CREATE TABLE TXXX (F1 VARCHAR, F2 VARCHAR, F3 VARCHAR, F4 VARCHAR, VAL NUMBER)";
        conn.createStatement().executeUpdate(wCreateTable);
        wCreateTable =
                "CREATE TABLE TABLEXXX_KO (F1,F2,VAL) AS (SELECT F1 , F2 , SUM(VAL) FROM TXXX GROUP BY F1,F2) WITH DATA";
        conn.createStatement().executeUpdate(wCreateTable);

        conn.close();
    }
}
