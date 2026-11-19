package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Opens a database encrypted by Access 2007, which requires Jackcess Encrypt on the classpath.
 */
@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class EncryptedTest extends UcanaccessBaseFileTest {

    @Test
    void connect_correctPassword_readsTables() throws Exception {
        try (UcanaccessConnection conn = buildConnection().withDbPath(getAccessTempPath()).withPassword("Test123").build();
            ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"})) {
            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
            assertThat(tables).containsExactly("Table1");
        }
    }

    @Test
    void connect_memoryFalse_encryptsMirror() throws Exception {
        String dbPath = getAccessTempPath();
        try (UcanaccessConnection conn = buildConnection().withDbPath(dbPath).withPassword("Test123")
            .withProp(Property.memory, false).build()) {
            DBReference ref = DBReferenceSingleton.getInstance().getReference(new File(dbPath));
            assertThat(ref.getDbIO()).isSameAs(conn.getDbIO());
            assertThat(ref.isEncryptHSQLDB()).isTrue();
        }
    }

    @Test
    void connect_wrongPassword_fails() {
        assertThatThrownBy(() -> buildConnection().withDbPath(getAccessTempPath()).withPassword("wrong").build())
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageContaining("Incorrect password provided");
    }
}
