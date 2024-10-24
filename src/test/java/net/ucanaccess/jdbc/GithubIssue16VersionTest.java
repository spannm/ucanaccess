package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.System.Logger.Level;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;

class GithubIssue16VersionTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testIssue16(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        String tbl = "`Donation Detail`";
        Date sqlDt = Date.valueOf(LocalDate.now());
        BigDecimal amt = new BigDecimal(0.01);

        executeStatements(String.join(" ", "CREATE TABLE", tbl, "("
                , "Donor_ID INTEGER,"
                , "Batch INTEGER, "
                , "[Donation Date] DATETIME,"
                , "[Deposit Date] DATETIME, "
                , "[Donation Type] TEXT(64),"
                , "[Amount] NUMERIC(12,3),"
                , "[Designation] TEXT(64)"
                , ")"));

        try (PreparedStatement ps = ucanaccess.prepareStatement(
            "INSERT INTO " + tbl + " (`Donor_ID`, `Batch`, `Donation Date`, `Deposit Date`, `Donation Type`, `Amount`, `Designation`) VALUES (?, ?, ?, ?, ?, ?, ?)")) {

            ps.setInt(1, 1);
            ps.setNull(2, Types.INTEGER);
            ps.setDate(3, sqlDt);
            ps.setDate(4, sqlDt);
            ps.setString(5, "donation_type");
            ps.setBigDecimal(6, amt);
            ps.setString(7, "designation");

            int rows = ps.executeUpdate();
            getLogger().log(Level.INFO, "Updated: {0} row(s)", rows);

            checkQuery("SELECT * FROM " + tbl + " WHERE Donor_ID=1",
                recs(rec(1, null, sqlDt, sqlDt, "donation_type", amt, "designation")));

            // executeStatements("DROP TABLE " + tbl);
        }
    }

}
