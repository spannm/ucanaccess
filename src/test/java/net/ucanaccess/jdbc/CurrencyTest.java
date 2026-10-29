package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.math.BigDecimal;
import java.sql.ResultSet;

@SuppressWarnings("checkstyle:MethodName")
class CurrencyTest extends UcanaccessBaseTest {

    // a currency column created by UCanAccess used to hold only values up to 9999.9999
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void createTable_currencyColumn_holdsAccessRange(AccessVersion accessVersion) throws Exception {
        init(accessVersion);

        String accdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();
        BigDecimal expected = new BigDecimal("922337203685477.5807");

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE t_currency (id LONG PRIMARY KEY, amount CURRENCY)");
            st.execute("INSERT INTO t_currency (id, amount) VALUES (1, 922337203685477.5807)");
            st.execute("INSERT INTO t_currency (id, amount) VALUES (2, -12345.5)");
        }

        ucanaccess.close();

        try (UcanaccessConnection conn = buildConnection()
                .withDbPath(accdbPath)
                .withImmediatelyReleaseResources()
                .build();
                UcanaccessStatement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT amount FROM t_currency ORDER BY id")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal(1)).isEqualByComparingTo(expected);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal(1)).isEqualByComparingTo("-12345.5");
        }
    }

}
