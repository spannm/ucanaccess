package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConcatNullsTrueTest extends AccessVersionDefaultTest {

    public ConcatNullsTrueTest(AccessVersion _accessVersion) {
        super(_accessVersion);
        // By default, any null value will cause the function to return null.
        // If the property is set false, then NULL values are replaced with empty strings.
        // see: http://hsqldb.org/doc/guide/builtinfunctions-chapt.html
        appendToJdbcURL(";concatnulls=true");
    }

    @Override
    public String getAccessPath() {
        return "testdbs/badDB.accdb";
    }

    @Test
    public void testConcat() throws Exception {
        checkQuery("SELECT 'aa2'& null FROM dual", new Object[][] {{null}});
    }

}
