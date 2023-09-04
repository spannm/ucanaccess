package net.ucanaccess.test.integration;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class PivotTest extends AccessVersionAllTest {

    public PivotTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/pivot.mdb";
    }

    @Test
    public void testPivot() throws SQLException, IOException, ParseException {
        Statement st = ucanaccess.createStatement();
        dumpQueryResult("SELECT * FROM Table1_trim");
        st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('O SOLE',1234.56,#2003-12-03#   )");
        st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('O SOLE MIO',134.46,#2003-12-03#   )");
        st.execute("INSERT INTO TABLE1(COD,VALUE,DT) VALUES ('STA IN FRUNTE A MEEE',1344.46,#2003-12-05#   )");
        initVerifyConnection();
        dumpQueryResult("SELECT * FROM Table1_trim");
        checkQuery("SELECT * FROM Table1_trim");
        st.close();
    }
}
