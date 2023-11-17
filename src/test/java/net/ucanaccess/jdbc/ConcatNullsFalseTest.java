package net.ucanaccess.jdbc;

import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ConcatNullsFalseTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "badDb.accdb";
    }

    protected UcanaccessConnectionBuilder buildConnection() {
        return super.buildConnection()
            .withProp(Property.concatNulls, false);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testConcat(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT 'aa2'& null FROM dual", new Object[][] {{"aa2"}});
    }

}
