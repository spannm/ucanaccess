package net.ucanaccess.jdbc;

import net.ucanaccess.converters.Metadata;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

class CharsetGBKTest extends UcanaccessBaseFileTest {

    @Test
    void testCharsetGBK() throws Exception {
        init();

        ucanaccess = buildConnection().withDbPath(getAccessTempPath()).withProp(Metadata.Property.charset, "GBK").build();
        String testSql ="SELECT * FROM table1";
        dumpQueryResult(testSql);

        try (PreparedStatement ps = ucanaccess.prepareStatement(testSql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            assertEquals("GBK编码", rs.getString(2));
        }

    }

}
