package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

class ConcatNullsFalseTest extends UcanaccessBaseTest {

    protected UcanaccessConnectionBuilder buildConnection() {
        return super.buildConnection().withConcatNulls(false);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testConcat(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT 'aa2'& null FROM dual", singleRec("aa2"));
    }

}
