package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class InsertBigTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE Tbig (id LONG, descr MEMO)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBig(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            String s = IntStream.range(0, 100000).mapToObj(i -> String.format("%05d", i)).collect(Collectors.joining("\r\n"));
            int id = 6666554;
            assertThat(s).hasSizeGreaterThanOrEqualTo(65536);
            st.execute("INSERT INTO Tbig (id, descr) VALUES(" + id + ", '" + s + "')");
            ResultSet rs = st.executeQuery("SELECT descr FROM Tbig WHERE id=" + id);
            rs.next();
            String retrieved = rs.getString(1);
            assertEquals(s, retrieved);
        }
    }

}
