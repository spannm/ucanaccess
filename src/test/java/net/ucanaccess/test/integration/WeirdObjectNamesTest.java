package net.ucanaccess.test.integration;

import java.sql.Statement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class WeirdObjectNamesTest extends AccessVersionAllTest {

    public WeirdObjectNamesTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/WeirdObjectNames.mdb";
    }

    @Test
    public void testTableNameEndsInQuestionMarks() throws Exception {
        Statement st = ucanaccess.createStatement();
        checkQuery("SELECT * FROM [19 MB 01 BEZAHLT ???]");
        st.close();
    }

}
