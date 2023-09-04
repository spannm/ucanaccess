package net.ucanaccess.test.integration;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Locale;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class ByteTest extends AccessVersionAllTest {

    public ByteTest(AccessVersion _accessVersion) {
        super(_accessVersion);
        Locale.setDefault(Locale.US);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements(
            "CREATE TABLE tblMain (ID int NOT NULL PRIMARY KEY,company TEXT NOT NULL, Closed byte); ",
            "INSERT INTO tblMain (id,company) VALUES(1, 'pippo')",
            "UPDATE tblMain SET closed=255");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("tblMain");
    }

    @Test
    public void testCreate() throws SQLException, IOException, ParseException {
        dumpQueryResult("SELECT * FROM tblMain");
        checkQuery("SELECT * FROM tblMain");
    }

}
