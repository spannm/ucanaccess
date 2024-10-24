package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

class WeirdObjectNamesTest extends UcanaccessBaseFileTest {

    @Test
    void testTableNameEndsInQuestionMarks() throws Exception {
        init();

        checkQuery("SELECT * FROM [19 MB 01 BEZAHLT ???]");
    }

}
