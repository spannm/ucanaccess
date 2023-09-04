package net.ucanaccess.test.integration;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Locale;

import org.hsqldb.error.ErrorCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.jdbc.UcanaccessErrorCodes;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class ExceptionCodeTest extends AccessVersionAllTest {

    public ExceptionCodeTest(AccessVersion _accessVersion) {
        super(_accessVersion);
        Locale.setDefault(Locale.US);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/bool.accdb";
    }

    @Test
    public void testVUKException() throws SQLException, IOException, ParseException {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();

            st.execute("INSERT INTO T(pk,b)  VALUES( 'pippo',true)");
            st.execute("INSERT INTO T(pk,b)  VALUES( 'pippo',true)");

        } catch (SQLException e) {

            assertEquals(e.getErrorCode(), -ErrorCode.X_23505);
            assertEquals(e.getSQLState(), "23505");
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    @Test
    public void testGenException() throws SQLException, IOException, ParseException {
        Statement st = null;
        try {
            throw new UcanaccessSQLException(ExceptionMessages.CONCURRENT_PROCESS_ACCESS.name(), "ko", 11111);

        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), 11111);
            assertEquals(e.getSQLState(), "ko");
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    @Test
    public void testGException() throws SQLException, IOException, ParseException {
        Statement st = null;
        try {
            DriverManager.getConnection(UcanaccessDriver.URL_PREFIX + "ciao ciao");

        } catch (SQLException _ex) {

            assertEquals(_ex.getErrorCode(), UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR);
            assertEquals(_ex.getSQLState(), UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR + "");
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

}
