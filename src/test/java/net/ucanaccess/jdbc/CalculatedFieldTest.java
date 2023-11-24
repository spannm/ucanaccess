package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.sql.ResultSet;

class CalculatedFieldTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "calculatedField.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode=Mode.INCLUDE, names = {"V2010"})
    void testFunctionBuiltInCall(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
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
                recs(rec("my", "my", "my", "my", "my"),
                     rec("myc", "myc", "myc", "myc", "myc"),
                     rec("mycat", "myc", "myc", "cat", "cat"),
                     rec("mycattom", "myc", "myc", "tom", "tom"),
                     rec("", "", "", "", "")));

            // inserting NULL into a calculated column containing a STRING formula is not permitted
            assertThatThrownBy(() -> st.execute("INSERT INTO clcdFlds (input) VALUES (null)"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("Value[NULL] 'null' cannot be converted to STRING");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode=Mode.INCLUDE, names = {"V2010"})
    void testCalculatedFieldNameContainsPercentSign(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            // definition of calculated column [%markup]: ([retail]/[wholesale]-1)*100
            st.execute("INSERT INTO products (wholesale, retail) VALUES (4, 5)");
            ResultSet rs = st.executeQuery("SELECT wholesale, retail, [%markup] FROM products WHERE [id]=3");
            rs.next();
            assertEquals(25.0, rs.getDouble("%markup"), 0.000001d);
        }
    }

}
