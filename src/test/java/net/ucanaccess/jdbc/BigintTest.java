package net.ucanaccess.jdbc;

import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

class BigintTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2016"})
    void testBigintInsert(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        String accdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();
        Long expected = 3000000000L;

        try (Statement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE table1 (entry TEXT(50) PRIMARY KEY, x BIGINT)");
            String sql = String.format("INSERT INTO table1 (entry, x) VALUES ('3 billion', %d)", expected);
            st.execute(sql);
        }

        ucanaccess.close();

        try (Connection conn = buildConnection()
                .withDbPath(accdbPath)
                .withProp(Property.immediatelyReleaseResources, true).build();
                Statement st2 = conn.createStatement();
                ResultSet rs = st2.executeQuery("SELECT x FROM table1 WHERE entry='3 billion'")) {
            rs.next();
            Long actual = rs.getLong("x");
            assertEquals(expected, actual);
        }
    }

}
