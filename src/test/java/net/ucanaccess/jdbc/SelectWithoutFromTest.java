package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;

/**
 * Runs SELECT statements without a FROM clause, which Access allows and HSQLDB does not.
 */
@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class SelectWithoutFromTest extends UcanaccessBaseTest {

    /**
     * Creates a database with a table holding two rows.
     */
    @BeforeEach
    void setUp() throws Exception {
        init(AccessVersion.getDefaultAccessVersion());
        executeStatements("CREATE TABLE t_nofrom (id COUNTER PRIMARY KEY, txt TEXT(10))",
            "INSERT INTO t_nofrom (txt) VALUES ('a')",
            "INSERT INTO t_nofrom (txt) VALUES ('b')");
    }

    @Test
    void executeQuery_expressions_oneRow() throws Exception {
        checkQuery("SELECT 1 + 1, 'x' & 'y', UCase('abc')", singleRec(2, "xy", "ABC"));
    }

    @Test
    void executeQuery_now_returnsTimestamp() throws Exception {
        try (UcanaccessStatement st = ucanaccess.createStatement();
            ResultSet rs = st.executeQuery("SELECT Now()")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getTimestamp(1)).isNotNull();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void executeQuery_domainFunction_oneRow() throws Exception {
        checkQuery("SELECT DCount('*', 't_nofrom'), DMax('txt', 't_nofrom')", singleRec(2, "b"));
    }

    @Test
    void executeQuery_union_rowPerPart() throws Exception {
        checkQuery("SELECT 1 UNION SELECT 2 ORDER BY 1", recs(rec(1), rec(2)));
    }

    @Test
    void executeQuery_subqueryWithoutFrom_filters() throws Exception {
        checkQuery("SELECT txt FROM t_nofrom WHERE id IN (SELECT 2)", singleRec("b"));
    }

    @Test
    void executeUpdate_insertSelect_insertsRow() throws Exception {
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            assertThat(st.executeUpdate("INSERT INTO t_nofrom (txt) SELECT 'c'")).isEqualTo(1);
        }
        checkQuery("SELECT txt FROM t_nofrom ORDER BY id", recs(rec("a"), rec("b"), rec("c")));
    }
}
