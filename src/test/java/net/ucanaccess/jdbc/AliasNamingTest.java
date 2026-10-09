package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

class AliasNamingTest extends UcanaccessBaseFileTest {

    @Test
    void testRegex2() throws SQLException {
        init();
        try (UcanaccessStatement st = ucanaccess.createStatement();
            ResultSet rs = st.executeQuery("SELECT SUM(category_id) AS `SUM(categories abc:category_id)` FROM `categories abc`")) {
            assertThat(rs.getMetaData().getColumnLabel(1)).isEqualTo("SUM(categories abc:category_id)");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getObject(1)).isNull();
        }
    }

}
