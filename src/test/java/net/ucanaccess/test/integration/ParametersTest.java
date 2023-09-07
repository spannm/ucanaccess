package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;

class ParametersTest extends UcanaccessTestBase {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "Parameters.accdb"; // Access 2007
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testParameters(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        dumpQueryResult("SELECT * FROM tq");
        dumpQueryResult("SELECT * FROM z");
        dumpQueryResult("SELECT * FROM [queryWithParameters]");
        dumpQueryResult("SELECT * FROM table(queryWithParameters(#1971-03-13#,'hi babe'))");
        checkQuery("SELECT COUNT(*) FROM [ab\"\"\"xxx]", 3);
        CallableStatement cs = ucanaccess.prepareCall("{call Insert_from_select_abxxx(?,?,?)}");
        cs.setString(1, "2");
        cs.setString(2, "YeaH!!!!");
        cs.setString(3, "u can see it works");
        cs.executeUpdate();

        dumpQueryResult("SELECT * FROM [ab\"\"\"xxx]");
        checkQuery("SELECT COUNT(*) FROM [ab\"\"\"xxx]", 6);
        // metaData();
        cs = ucanaccess.prepareCall("{call InsertWithFewParameters(?,?,?)}");

        cs.setString(1, "555");
        cs.setString(2, "YeaH!ddd!!!");
        cs.setDate(3, new java.sql.Date(System.currentTimeMillis()));
        cs.executeUpdate();
        cs.executeUpdate();
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM [table 1]");

        dumpQueryResult("SELECT * FROM Membership");
        // test saved UPDATE query with PARAMETERS
        cs = ucanaccess.prepareCall("{call UpdateMembershipLevel(?,?)}");
        cs.setString(1, "Gold");
        cs.setInt(2, 1);
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM Membership");
        // same again, but with space after the procedure name
        cs = ucanaccess.prepareCall("{call UpdateMembershipLevel (?,?)}");
        cs.setString(1, "Platinum");
        cs.setInt(2, 1);
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM Membership");

        cs = ucanaccess.prepareCall("{call InsertNewMembership(?,?,?)}");
        cs.setString(1, "Thompson");
        cs.setString(2, "Gord");
        cs.setString(3, "Basic");
        cs.executeUpdate();
        checkQuery("SELECT @@IDENTITY", 2); // verify that we can retrieve the AutoNumber ID
        cs.executeUpdate();
        checkQuery("SELECT @@IDENTITY", 3); // and again, just to be sure
        cs = ucanaccess.prepareCall("{call UpdateWhere(?,?)}");
        cs.setString(1, "updated");
        cs.setString(2, "3x");
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM [table 1]");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testLocalTimeParameters(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        final LocalTime desiredTime = LocalTime.of(12, 0, 1);
        final String expectedText = "one second past noon";

        ResultSet rs = null;

        PreparedStatement ps = ucanaccess.prepareStatement("SELECT Description FROM TimeValues WHERE TimeValue=?");
        ps.setObject(1, desiredTime);
        rs = ps.executeQuery();
        rs.next();
        assertEquals(expectedText, rs.getString("Description"));

        CallableStatement cs = ucanaccess.prepareCall("{CALL SelectTimeValueUsingParameter(?)}");
        cs.setObject(1, desiredTime);
        rs = cs.executeQuery();
        rs.next();
        assertEquals(expectedText, rs.getString("Description"));
    }

}
