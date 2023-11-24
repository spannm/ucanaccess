package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PasswordTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "pwd.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testPassword(AccessVersion _accessVersion) throws Exception {

        assertThatThrownBy(() -> init(_accessVersion))
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageContaining("Password authentication failed");

        try (UcanaccessConnection conn = new UcanaccessConnectionBuilder()
            .withDbPath(getAccessTempPath())
            .withPassword("ucanaccess")
            .build()) {
            assertNotNull(conn);
        }
    }
}
