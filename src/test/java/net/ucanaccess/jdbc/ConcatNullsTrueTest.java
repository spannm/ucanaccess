package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

class ConcatNullsTrueTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "badDb.accdb";
    }

    @Override
    protected UcanaccessConnectionBuilder buildConnection() {
        // By default, any null value will cause the function to return null.
        // If the property is set false, then NULL values are replaced with empty strings.
        // see: http://hsqldb.org/doc/guide/builtinfunctions-chapt.html
        return super.buildConnection()
            .withConcatNulls(true);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testConcat(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        checkQuery("SELECT 'aa2'& null FROM dual", recs(rec((Object) null)));
    }

}
