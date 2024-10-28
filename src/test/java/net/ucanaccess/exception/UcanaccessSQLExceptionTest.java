package net.ucanaccess.exception;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UcanaccessSQLExceptionTest extends AbstractBaseTest {

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource(delimiter = '|', value = {
        "UCAExc:| UCAExc:",
        "bogus| UCAExc:::[0-9]\\.[0-9][0-9\\.]*(?:-SNAPSHOT)? bogus",
        "''| UCAExc:::[0-9]\\.[0-9][0-9\\.]*(?:-SNAPSHOT)? \\(n\\/a\\)",
        "| UCAExc:::[0-9]\\.[0-9][0-9\\.]*(?:-SNAPSHOT)? \\(n\\/a\\)"
    })
    void testAddVersionInfo(String _message, CharSequence _expectedPattern) {
        assertThat(new UcanaccessSQLException().addVersionInfo(_message))
            .matches(_expectedPattern);
    }

}
