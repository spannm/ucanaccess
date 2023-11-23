package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class WeirdObjectNamesTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "weirdObjectNames.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testTableNameEndsInQuestionMarks(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        checkQuery("SELECT * FROM [19 MB 01 BEZAHLT ???]");
    }

}
