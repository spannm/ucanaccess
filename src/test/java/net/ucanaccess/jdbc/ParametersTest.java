package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;

class ParametersTest extends UcanaccessBaseFileTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource(include = "V2007")
    void testParameters(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        dumpQueryResult("SELECT * FROM tq");
        dumpQueryResult("SELECT * FROM z");
        dumpQueryResult("SELECT * FROM [queryWithParameters]");
        dumpQueryResult("SELECT * FROM table(queryWithParameters(#1971-03-13#,'hi babe'))");
        checkQuery("SELECT COUNT(*) FROM [ab\"\"\"xxx]", singleRec(3));

        CallableStatement cs = ucanaccess.prepareCall("{call Insert_from_select_abxxx(?,?,?)}");
        cs.setString(1, "2");
        cs.setString(2, "YeaH!!!!");
        cs.setString(3, "u can see it works");
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM [ab\"\"\"xxx]");
        checkQuery("SELECT COUNT(*) FROM [ab\"\"\"xxx]", singleRec(6));

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
        checkQuery("SELECT @@IDENTITY", singleRec(2)); // verify that we can retrieve the AutoNumber ID
        cs.executeUpdate();
        checkQuery("SELECT @@IDENTITY", singleRec(3)); // and again, just to be sure

        cs = ucanaccess.prepareCall("{call UpdateWhere(?,?)}");
        cs.setString(1, "updated");
        cs.setString(2, "3x");
        cs.executeUpdate();
        dumpQueryResult("SELECT * FROM [table 1]");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource(include = "V2007")
    void testLocalTimeParameters(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        LocalTime desiredTime = LocalTime.of(12, 0, 1);
        String expectedText = "one second past noon";

        try (PreparedStatement ps = ucanaccess.prepareStatement("SELECT Description FROM TimeValues WHERE TimeValue=?")) {
            ps.setObject(1, desiredTime);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertEquals(expectedText, rs.getString("Description"));
        }

        try (CallableStatement cs = ucanaccess.prepareCall("{CALL SelectTimeValueUsingParameter(?)}")) {
            cs.setObject(1, desiredTime);
            ResultSet rs2 = cs.executeQuery();
            rs2.next();
            assertEquals(expectedText, rs2.getString("Description"));
        }
    }

}
