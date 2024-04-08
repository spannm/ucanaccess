package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

class WeirdObjectNamesTest extends UcanaccessBaseFileTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testTableNameEndsInQuestionMarks(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        checkQuery("SELECT * FROM [19 MB 01 BEZAHLT ???]");
    }

}
