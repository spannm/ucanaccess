package net.ucanaccess.test.integration;

import java.io.File;
import java.sql.Connection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class PasswordTest extends AccessVersionAllTest {

    public PasswordTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testPassword() throws Exception {
        File dbFile = copyResourceToTempFile("testdbs/pwd.mdb");
        Connection ucanaccessConnection = null;
        try {
            ucanaccessConnection = getUcanaccessConnection(dbFile.getAbsolutePath());
        } catch (Exception _ex) {
            assertContains(_ex.getMessage(), "Password authentication failed");
        }
        assertNull(ucanaccessConnection);

        setPassword("ucanaccess");
        ucanaccessConnection = getUcanaccessConnection();
        ucanaccessConnection.close();
        assertNotNull(ucanaccessConnection);
    }
}
