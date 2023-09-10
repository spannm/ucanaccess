package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessSQLException;
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
            // definition of calculated columns:
            // clcd1: LEFT([input];3)
            // clcd2: LEFT$([input];3)
            // clcd3: RIGHT([input];3)
            // clcd4: RIGHT$([input];3)
            
            st.execute("INSERT INTO clcdFlds (input) VALUES ('my')");
            st.execute("INSERT INTO clcdFlds (input) VALUES ('myc')");
            st.execute("INSERT INTO clcdFlds (input) VALUES ('mycat')");
            st.execute("INSERT INTO clcdFlds (input) VALUES ('mycattom')");
            st.execute("INSERT INTO clcdFlds (input) VALUES ('')");

            dumpQueryResult("SELECT * FROM clcdFlds");

            checkQuery("SELECT input, clcd1, clcd2, clcd3, clcd4 FROM clcdFlds ORDER BY id",
                new String[][] {{"my", "my", "my", "my", "my"},
                                {"myc", "myc", "myc", "myc", "myc"},
                                {"mycat", "myc", "myc", "cat", "cat"},
                                {"mycattom", "myc", "myc", "tom", "tom"},
                                {"", "", "", "", ""}});

            // inserting NULL into a calculated column containing a STRING formula is not permitted
            UcanaccessSQLException ex = assertThrows(UcanaccessSQLException.class, () -> st.execute("INSERT INTO clcdFlds (input) VALUES (null)"));
            assertContains(ex.getMessage(), "Value[NULL] 'null' cannot be converted to STRING");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode=Mode.INCLUDE, names = {"V2010"})
    void testCalculatedFieldNameContainsPercentSign(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            // definition of calculated column [%markup]: ([retail]/[wholesale]-1)*100
            st.execute("INSERT INTO products (wholesale, retail) VALUES (4, 5)");
            ResultSet rs = st.executeQuery("SELECT wholesale, retail, [%markup] FROM products WHERE [id]=3");
            rs.next();
            assertEquals(25.0, rs.getDouble("%markup"), 0.000001d);
        }
    }

}
