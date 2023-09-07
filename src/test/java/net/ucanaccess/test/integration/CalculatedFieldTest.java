package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.sql.ResultSet;
import java.sql.Statement;

class CalculatedFieldTest extends UcanaccessTestBase {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "calculatedField.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode=Mode.INCLUDE, names = {"V2010"})
    void testFunctionBuiltInCall(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO T (c1) VALUES ('my')");
            st.execute("INSERT INTO T (c1) VALUES ('myc')");
            st.execute("INSERT INTO T (c1) VALUES ('mycat')");
            st.execute("INSERT INTO T (c1) VALUES ('mycattom')");
            st.execute("INSERT INTO T (c1) VALUES (null)");
            dumpQueryResult("SELECT * FROM T");
            checkQuery("SELECT c2, c3, c4, c5 FROM T ORDER BY id",
                new String[][] {{"my", "my", "my", "my"}, {"myc", "myc", "myc", "myc"}, {"myc", "myc", "cat", "cat"}, {"myc", "myc", "tom", "tom"}, {null, null, null, null}});
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode=Mode.INCLUDE, names = {"V2010"})
    void testCalculatedFieldNameContainsPercentSign(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO Product (wholesale, retail) VALUES (4, 5)");
            ResultSet rs = st.executeQuery("SELECT wholesale, retail, [%markup] FROM Product WHERE [ID]=3");
            rs.next();
            assertEquals(25.0, rs.getDouble("%markup"), 0.000001d);
        }
    }

}
