package net.ucanaccess.jdbc;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UcanaccessSQLExceptionTest  extends AbstractBaseTest {

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource(delimiter = ';', value = {
        "UCAExc:; UCAExc:",
        "bogus; UCAExc:::5.1.0 bogus",
        "''; UCAExc:::5.1.0 (n/a)",
        "; UCAExc:::5.1.0 (n/a)"
    })
    void testAddVersionInfo(String _message, String _expectedResult) {
        assertEquals(_expectedResult, new UcanaccessSQLException().addVersionInfo(_message));
    }

}
