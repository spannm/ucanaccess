package net.ucanaccess.converters;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests {@link SQLConverter#addFromDual(String)}.
 */
@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class AddFromDualTest extends AbstractBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {
        "SELECT NOW()                                | SELECT NOW() FROM DUAL",
        "select 1, 'a'                               | select 1, 'a' FROM DUAL",
        "SELECT NOW();                               | SELECT NOW() FROM DUAL;",
        "SELECT 1 AS x ORDER BY 1                    | SELECT 1 AS x FROM DUAL ORDER BY 1",
        "SELECT 1 UNION SELECT 2                     | SELECT 1 FROM DUAL UNION SELECT 2 FROM DUAL",
        "SELECT (SELECT MAX(a) FROM t)               | SELECT (SELECT MAX(a) FROM t) FROM DUAL",
        "SELECT a FROM t WHERE b IN (SELECT 1)       | SELECT a FROM t WHERE b IN (SELECT 1 FROM DUAL)",
        "INSERT INTO t (a, b) SELECT 1, 'x'          | INSERT INTO t (a, b) SELECT 1, 'x' FROM DUAL",
        "SELECT 'no FROM here' AS \"SELECT\"         | SELECT 'no FROM here' AS \"SELECT\" FROM DUAL"
    })
    void addFromDual_selectWithoutFrom_addsFromDual(String sql, String expected) {
        assertThat(SQLConverter.addFromDual(sql)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {
        "SELECT * FROM t",
        "SELECT a FROM t WHERE b IN (SELECT c FROM u)",
        "SELECT [select from] FROM t",
        "SELECT a.selected, a.fromage FROM t a",
        "INSERT INTO t VALUES (1, 'SELECT 1')",
        "UPDATE t SET a = 1",
        "CREATE TABLE t (a INTEGER)"
    })
    void addFromDual_fromPresentOrNoSelect_unchanged(String sql) {
        assertThat(SQLConverter.addFromDual(sql)).isEqualTo(sql);
    }
}
