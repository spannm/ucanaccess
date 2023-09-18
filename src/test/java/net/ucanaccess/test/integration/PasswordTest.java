package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.sql.Connection;

class PasswordTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testPassword(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        File dbFile = copyResourceToTempFile(TEST_DB_DIR + "pwd.mdb");
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
