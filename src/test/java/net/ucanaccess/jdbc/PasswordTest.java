package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

class PasswordTest extends UcanaccessBaseFileTest {

    @Test
    void testPassword() throws Exception {
        assertThatThrownBy(() -> init())
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageContaining("Authentication failed");

        try (UcanaccessConnection conn = new UcanaccessConnectionBuilder()
            .withDbPath(getAccessTempPath())
            .withPassword("ucanaccess")
            .build()) {
            assertNotNull(conn);
        }
    }

}
