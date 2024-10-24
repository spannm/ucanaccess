package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class PivotTest extends UcanaccessBaseFileTest {

    @Test
    void testPivot() throws SQLException {
        init();

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            dumpQueryResult("SELECT * FROM q_trim");
            executeStatements(st,
                "INSERT INTO t_pivot(c_cod, c_val, c_dt) VALUES('O SOLE', 1234.56, #2003-12-03#)",
                "INSERT INTO t_pivot(c_cod, c_val, c_dt) VALUES('O SOLE MIO', 134.46, #2003-12-03#)",
                "INSERT INTO t_pivot(c_cod, c_val, c_dt) VALUES('STA IN FRUNTE A MEEE', 1344.46, #2003-12-05#)");
            initVerifyConnection();

            dumpQueryResult("SELECT * FROM q_trim");
            checkQuery("SELECT * FROM q_trim");
        }
    }
}
