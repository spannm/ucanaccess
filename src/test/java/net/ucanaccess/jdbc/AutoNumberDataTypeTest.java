package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.ResultSet;

/**
 * Test to verify fix for <a href="https://github.com/spannm/ucanaccess/issues/22">issue #22</a> (Github) "The SQL Type of the AutoNumber column is different when loading an existing table and immediately after executing the CREATE TABLE statement".
 * </p>
 * Thanks to <a href="https://github.com/Takumi-Inoue-hulft">Takumi-Inoue-hulft</a> for reporting.
 */
class AutoNumberDataTypeTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource(include = "V2016")
    void testAutoNumberDataType(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        Object colVal1;
        int columnType1;

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE t_autonumber ( c_autonumber COUNTER, c_text TEXT )");
            st.execute("INSERT INTO t_autonumber (c_text) VALUES ('" + getShortTestMethodName() + "')");

            ResultSet rs = st.executeQuery("SELECT * FROM t_autonumber");
            rs.next();

            colVal1 = rs.getObject(1);
            columnType1 = rs.getMetaData().getColumnType(1);
        }

        ucanaccess.close();

        String accdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();

        try (UcanaccessConnection conn = buildConnection()
                .withDbPath(accdbPath)
                .withImmediatelyReleaseResources()
                .build();
                UcanaccessStatement st = conn.createStatement()) {

            ResultSet rs = st.executeQuery("SELECT * FROM t_autonumber");
            rs.next();

            Object colVal2 = rs.getObject(1);
            int columnType2 = rs.getMetaData().getColumnType(1);

            assertEquals(colVal1, colVal2, "Column value mismatch");
            assertEquals(columnType1, columnType2, "Column type mismatch");
        }

    }

}
