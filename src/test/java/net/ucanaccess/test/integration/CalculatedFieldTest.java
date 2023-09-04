package net.ucanaccess.test.integration;

import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2010Test;

@RunWith(Parameterized.class)
public class CalculatedFieldTest extends AccessVersion2010Test {

    public CalculatedFieldTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/calculatedField.accdb";
    }

    @Test
    public void testFunctionBuiltInCall() throws Exception {
        Statement st = null;
        st = ucanaccess.createStatement();
        st.execute("INSERT INTO T (c1) VALUES ('my')");
        st.execute("INSERT INTO T (c1) VALUES ('myc')");
        st.execute("INSERT INTO T (c1) VALUES ('mycat')");
        st.execute("INSERT INTO T (c1) VALUES ('mycattom')");
        st.execute("INSERT INTO T (c1) VALUES (null)");
        dumpQueryResult("SELECT * FROM T");
        checkQuery("select c2,c3,c4,c5 from T order by id",
                new String[][] { { "my", "my", "my", "my" }, { "myc", "myc", "myc", "myc" },
                        { "myc", "myc", "cat", "cat" }, { "myc", "myc", "tom", "tom" },
                        { null, null, null, null } });

        st.close();
    }

    @Test
    public void testCalculatedFieldNameContainsPercentSign() throws Exception {
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO Product (wholesale, retail) VALUES (4, 5)");
        ResultSet rs = st.executeQuery("SELECT wholesale, retail, [%markup] FROM Product WHERE [ID]=3");
        rs.next();
        assertEquals(25.0, rs.getDouble("%markup"), 0.000001d);
        st.close();
    }

}
