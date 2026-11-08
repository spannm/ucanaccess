package net.ucanaccess.converters;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.exception.UcanaccessSQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Timestamp;
import java.util.Locale;
import java.util.Objects;

/**
 * Tests {@link AccessFormat} with a German locale. The expected values come from Access with German regional
 * settings, evaluated with Eval(Format(...)).
 */
@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class AccessFormatTest {

    private static Locale locale;

    /**
     * Switches to the German locale the expected values were taken with.
     */
    @BeforeAll
    static void setLocale() {
        locale = Locale.getDefault();
        Locale.setDefault(Locale.GERMANY);
    }

    /**
     * Restores the locale.
     */
    @AfterAll
    static void resetLocale() {
        Locale.setDefault(Objects.requireNonNullElseGet(locale, Locale::getDefault));
    }

    /**
     * Formats numbers with sections, literals, placeholders, scaling, exponents and named formats.
     */
    @ParameterizedTest(name = "[{index}] Format({0}, \"{1}\") = \"{2}\"")
    @CsvSource(delimiter = '|', value = {
        "5                | 0;(0);\\z\\e\\r\\o          | 5",
        "-5               | 0;(0);\\z\\e\\r\\o          | (5)",
        "-0.4             | 0;(0);\\z\\e\\r\\o          | zero",
        "0.4              | 0;(0);\\z\\e\\r\\o          | zero",
        "-5               | 0;;\\z                      | -5",
        "0                | 0;-0;                       | 0",
        "-5               | 0;                          | -5",
        "5                | ;\\n\\e\\g                  | ''",
        "-1234.5          | #,##0.00;[Red]-#,##0.00     | -1.234,50",
        "5                | 0[Red]                      | 5",
        "5                | 0\" kg\"                    | 5 kg",
        "1234.5           | #,##0.00 \"EUR\"            | 1.234,50 EUR",
        "532              | 0.0 \\k\\g                  | 532,0 kg",
        "12               | \\#0                        | #12",
        "5                | 0\\%                        | 5%",
        "0.5              | 0.0\\%                      | 0,5%",
        "1234567          | 000-0000                    | 123-4567",
        "5                | \\0                         | 0",
        "5                | 0\\                         | 5",
        "5                | 0\\\\                       | 5\\",
        "-1234.5678       | \\$0.00                     | -$1234,57",
        "-1234.5678       | (0)                         | -(1235)",
        "48.8091          | 000                         | 049",
        "0.5              | #.##                        | ,5",
        "0                | #                           | ''",
        "0                | #.##                        | ,",
        "0                | 0.##                        | 0,",
        "123.456          | 0.0#                        | 123,46",
        "1234567          | #,##0,                      | 1.235",
        "1234567          | 0,,                         | 1",
        "1234567          | #,                          | 1235",
        "1234567          | 0.0,,                       | 1234567,0",
        "-1234.5678       | 0.00E+00                    | -1,23E+03",
        "1234.5           | 0.00E-00                    | 1,23E03",
        "0.00012          | 0.00E-00                    | 1,20E-04",
        "1234.5           | ##0.0E+0                    | 123,5E+1",
        "1234.5           | 0e+0                        | 1e+3",
        "-0.4             | 0                           | 0",
        "-0.04            | 0.0                         | 0,0",
        "-0.4             | #                           | ''",
        "-0.4             | #,##0                       | 0",
        "1.005            | 0.00                        | 1,01",
        "2.675            | 0.00                        | 2,68",
        "-2.5             | 0                           | -3",
        "1234567.5        | Standard                    | 1.234.567,50",
        "-1.5             | Fixed                       | -1,50",
        "-1234.5678       | Percent                     | -123456,78%",
        "-1234.5678       | Scientific                  | -1,23E+03",
        "1.5              | Currency                    | 1,50 €",
        "-1234.5678       | General Number              | -1234,5678",
        "0.3333333333333333 | General Number            | 0,333333333333333",
        "9007199254740992 | ''                          | 9,00719925474099E+15",
        "1E16             | ''                          | 10000000000000000",
        "-1234.5678       | @@@                         | -1234,5678",
        "123              | @@@@@                       | '  123'",
        "0.75             | hh:nn                       | 18:00",
        "45000.25         | General Date                | 15.03.2023 06:00:00",
        "45000.5          | dd.mm.yyyy hh:nn            | 15.03.2023 12:00",
        "-1               | dd.mm.yyyy                  | 29.12.1899"
    })
    void format_number_likeAccess(double value, String format, String expected) throws UcanaccessSQLException {
        assertThat(AccessFormat.format(value, format, 1, 1)).isEqualTo(expected);
    }

    /**
     * Formats texts with the placeholders @ and &amp;, the flags &lt;, &gt; and ! and texts that are numbers or dates.
     */
    @ParameterizedTest(name = "[{index}] Format(\"{0}\", \"{1}\") = \"{2}\"")
    @CsvSource(delimiter = '|', value = {
        "abc      | @@@@@             | '  abc'",
        "abc      | !@@@@@            | 'abc  '",
        "abcdef   | @@@               | abcdef",
        "abcdef   | !@@@              | def",
        "ab       | (@@@) @@          | (   ) ab",
        "abc      | @-@               | a-bc",
        "abc      | &-&               | a-bc",
        "abc      | !&&&&&            | abc",
        "ABC      | <@@@              | abc",
        "abc      | >&&&&             | ABC",
        "abc      | >                 | ABC",
        "''       | @;\\l\\e\\e\\r    | leer",
        "x        | @;\\l\\e\\e\\r    | x",
        "abc      | 0                 | abc",
        "abc      | Yes/No            | abc",
        "1.5      | 0.00              | 15,00",
        "12,5     | 0.00              | 12,50",
        "5.3.2026 | dd.mm.yyyy        | 05.03.2026",
        "18:15    | hh:nn:ss          | 18:15:00"
    })
    void format_text_likeAccess(String value, String format, String expected) throws UcanaccessSQLException {
        assertThat(AccessFormat.format(value, format, 1, 1)).isEqualTo(expected);
    }

    /**
     * Formats Null: only the fourth number section and the second text section show something.
     */
    @ParameterizedTest(name = "[{index}] Format(Null, \"{0}\") = \"{1}\"")
    @CsvSource(delimiter = '|', value = {
        "0;(0);\\z\\e\\r\\o;\\n\\u\\l\\l | null",
        "@;\\l\\e\\e\\r                  | leer",
        "0.00                            | ''",
        "''                              | ''"
    })
    void format_null_likeAccess(String format, String expected) throws UcanaccessSQLException {
        assertThat(AccessFormat.format(null, format, 1, 1)).isEqualTo(expected);
    }

    /**
     * Formats dates with date tokens, literals and AM/PM.
     */
    @ParameterizedTest(name = "[{index}] Format(#{0}#, \"{1}\") = \"{2}\"")
    @CsvSource(delimiter = '|', value = {
        "2026-03-05 18:15:09 | ddddd                | 05.03.2026",
        "2026-03-05 18:15:09 | dddddd               | Donnerstag, 5. März 2026",
        "2026-03-05 07:08:09 | ddd                  | Do",
        "2026-03-05 18:15:09 | tttt                 | tttt",
        "2026-03-05 18:15:09 | ttttt                | 18:15:09",
        "2026-03-05 18:15:09 | hh:nn AM/PM          | 06:15 PM",
        "2026-03-05 18:15:09 | h:nn a/p             | 6:15 p",
        "2026-03-05 07:08:09 | h:nn am/pm           | 7:08 am",
        "1899-12-30 00:30:00 | h:nn AM/PM           | 12:30 AM",
        "1899-12-30 12:00:00 | h AM/PM              | 12 PM",
        "1899-12-30 00:00:00 | hh:nn A/P            | 12:00 A",
        "2026-03-05 18:15:09 | hh:mm                | 18:15",
        "2026-03-05 18:15:09 | h:m                  | 18:15",
        "2026-03-05 18:15:09 | mm:ss                | 03:09",
        "2026-03-05 18:15:09 | m/d/yy               | 3.5.26",
        "2026-03-05 18:15:09 | dd\\.mm\\.yyyy       | 05.03.2026",
        "2026-03-05 07:08:09 | mm\\/dd              | 03/05",
        "2026-03-05 18:15:09 | mmmmm                | März3",
        "2026-03-05 18:15:09 | yyyy\"-W\"ww          | 2026-W10",
        "2026-03-05 18:15:09 | \"Q\"q yyyy          | Q1 2026",
        "2026-03-05 18:15:09 | c                    | 05.03.2026 18:15:09",
        "2026-03-05 00:00:00 | c                    | 05.03.2026",
        "1899-12-30 18:15:09 | c                    | 18:15:09",
        "2026-03-05 18:15:09 | y                    | 64",
        "2026-03-05 07:08:09 | w                    | 5",
        "2026-03-05 07:08:09 | ww                   | 10",
        "2026-03-05 18:15:09 | oooo                 | oooo",
        "2026-03-05 18:15:09 | aaaa                 | aaaa"
    })
    void format_date_likeAccess(String value, String format, String expected) throws UcanaccessSQLException {
        assertThat(AccessFormat.format(Timestamp.valueOf(value), format, 1, 1)).isEqualTo(expected);
    }

    /**
     * Formats the day and week numbers with the arguments firstdayofweek and firstweekofyear.
     */
    @ParameterizedTest(name = "[{index}] Format(#{0}#, \"{1}\", {2}, {3}) = \"{4}\"")
    @CsvSource(delimiter = '|', value = {
        "2026-03-05 | w  | 2 | 1 | 4",
        "2026-03-05 | w  | 0 | 1 | 4",
        "2026-03-05 | ww | 2 | 1 | 10",
        "2026-01-01 | ww | 2 | 1 | 1",
        "2026-01-01 | ww | 2 | 2 | 1",
        "2026-01-01 | ww | 2 | 3 | 52",
        "2027-01-01 | ww | 2 | 2 | 53",
        "2027-01-01 | ww | 2 | 3 | 52",
        "2026-12-31 | ww | 2 | 3 | 52"
    })
    void format_weekArguments_likeAccess(String value, String format, int firstDayOfWeek, int firstWeekOfYear, String expected)
        throws UcanaccessSQLException {
        assertThat(AccessFormat.format(Timestamp.valueOf(value + " 00:00:00"), format, firstDayOfWeek, firstWeekOfYear))
            .isEqualTo(expected);
    }
}
