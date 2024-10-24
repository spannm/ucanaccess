package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.lang.System.Logger.Level;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;

class GithubIssue16Test extends UcanaccessBaseFileTest {

    @Test
    void testIssue16() throws SQLException {
        init();

        String tbl = "`Donation Detail`";
        Date sqlDt = Date.valueOf(LocalDate.now());
        BigDecimal amt = new BigDecimal(0.01);

        try (PreparedStatement ps = ucanaccess.prepareStatement(
            "INSERT INTO " + tbl + " (`Donor_ID`, `Batch`, `Donation Date`, `Deposit Date`, `Donation Type`, `Amount`, `Designation`, `Notes`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {

            ps.setInt(1, 1);
            ps.setNull(2, Types.INTEGER);
            ps.setDate(3, sqlDt);
            ps.setDate(4, sqlDt);
            ps.setString(5, "donation_type");
            ps.setBigDecimal(6, amt);
            ps.setString(7, "designation");
            ps.setString(8, null);

            int rows = ps.executeUpdate();
            getLogger().log(Level.INFO, "Updated: {0} row(s)", rows);

            checkQuery("SELECT * FROM " + tbl + " WHERE Donor_ID=1",
                recs(rec(1, null, sqlDt, sqlDt, "donation_type", amt, "designation", null)));
        }
    }

}
