package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
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

        setPassword("");
        assertThatThrownBy(() -> getUcanaccessConnection(dbFile.getAbsolutePath()))
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageContaining("Password authentication failed");

        setPassword("ucanaccess");
        try (Connection ucanaccessConnection = getUcanaccessConnection()) {
            assertNotNull(ucanaccessConnection);
        }
    }
}
