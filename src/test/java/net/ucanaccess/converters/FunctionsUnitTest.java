package net.ucanaccess.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import net.ucanaccess.exception.InvalidFunctionParameterException;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.Callable;

/**
 * Unit tests calling the static methods of {@link Functions} directly, without a database.
 */
@SuppressWarnings("checkstyle:MethodName")
class FunctionsUnitTest extends AbstractBaseTest {

    private static final String    DT1 = "2026-01-15 10:30:45";
    // both dates in winter time, so differences do not depend on daylight saving time
    private static final String    DT2 = "2027-02-20 12:45:45";
    private static final Timestamp TS1 = Timestamp.valueOf(DT1);
    private static final Timestamp TS2 = Timestamp.valueOf(DT2);

    private static Locale locale;

    @BeforeAll
    static void setLocale() {
        locale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @AfterAll
    static void resetLocale() {
        Locale.setDefault(Objects.requireNonNullElseGet(locale, Locale::getDefault));
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource(delimiter = '|', value = {
        "    |",
        "''  |",
        "AB  | 65"
    })
    void asc_string_firstCharCode(String value, Integer expected) {
        assertThat(Functions.asc(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource(delimiter = '|', value = {
        "    |",
        "''  |",
        "AB  | 65",
        "\u20ac   | 8364",
        "\uffff   | -1"
    })
    void ascW_string_unicodeAsShort(String value, Integer expected) {
        assertThat(Functions.ascW(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource({
        "65,     A",
        "8364,   \u20ac",
        "-1,     \uffff",
        "65535,  \uffff"
    })
    void chrW_code_character(int code, String expected) {
        assertThat(Functions.chrW(code)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(ints = {65536, -32769})
    void chrW_outOfRange_throws(int code) {
        assertThatThrownBy(() -> Functions.chrW(code)).isInstanceOf(UcanaccessRuntimeException.class);
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource({
        "8,   10",
        "-1,  37777777777",
        "8.5, 10",
        "9.5, 12"
    })
    void oct_number_roundedOctal(double value, String expected) {
        assertThat(Functions.oct(value)).isEqualTo(expected);
    }

    @Test
    void equals_objectsAndArrays_comparesInOrder() {
        assertThat(Functions.equals(null, "a")).isFalse();
        assertThat(Functions.equals("a", 1)).isFalse();
        assertThat(Functions.equals("a", "a")).isTrue();
        assertThat(Functions.equals(new String[] {"a", "b"}, new String[] {"a", "b"})).isTrue();
        assertThat(Functions.equals(new String[] {"a", "b"}, new String[] {"b", "a"})).isFalse();
    }

    @Test
    void equalsIgnoreOrder_objectsAndArrays_comparesContent() {
        assertThat(Functions.equalsIgnoreOrder("a", null)).isFalse();
        assertThat(Functions.equalsIgnoreOrder("a", 1)).isFalse();
        assertThat(Functions.equalsIgnoreOrder("a", "a")).isTrue();
        assertThat(Functions.equalsIgnoreOrder(new String[] {"a", "b"}, new String[] {"b", "a"})).isTrue();
        assertThat(Functions.equalsIgnoreOrder(new String[] {"a", "b"}, new String[] {"a", "c"})).isFalse();
    }

    @Test
    void contains_arrayAndElements_checksAllContained() {
        String[] ab = {"a", "b"};
        assertThat(Functions.contains(null, "a")).isFalse();
        assertThat(Functions.contains(ab, null)).isFalse();
        assertThat(Functions.contains("a", "a")).isFalse();
        assertThat(Functions.contains(ab, "a")).isTrue();
        assertThat(Functions.contains(ab, new String[] {"b"})).isTrue();
        assertThat(Functions.contains(ab, new String[] {"a", "c"})).isFalse();
    }

    @Test
    void cbool_variousTypes_convertsToBoolean() {
        assertThat(Functions.cbool((BigDecimal) null)).isFalse();
        assertThat(Functions.cbool(BigDecimal.ZERO)).isFalse();
        assertThat(Functions.cbool(BigDecimal.ONE)).isTrue();
        assertThat(Functions.cbool(Boolean.TRUE)).isTrue();
        assertThat(Functions.cbool("true")).isTrue();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(booleans = {true, false})
    void booleanToNumber_yesNo_minusOneOrZero(boolean value) {
        int expected = value ? -1 : 0;
        assertThat(Functions.cint(value)).isEqualTo((short) expected);
        assertThat(Functions.mint(value)).isEqualTo((short) expected);
        assertThat(Functions.clong(value)).isEqualTo(expected);
    }

    @Test
    void conversions_matchingType_returnValueUnchanged() throws UcanaccessSQLException {
        assertThat(Functions.sqr(9)).isEqualTo(3.0);
        assertThat(Functions.cdbl(1.5)).isEqualTo(1.5);
        assertThat(Functions.clng(Integer.valueOf(7))).isEqualTo(7);
        assertThat(Functions.clong(Integer.valueOf(7))).isEqualTo(7);
        assertThat(Functions.clng("1,234.5")).isEqualTo(1234);
        assertThat(Functions.cstr("abc")).isEqualTo("abc");
        assertThat(Functions.cstr((Timestamp) null)).isNull();
        assertThat(Functions.cstr((Boolean) null)).isNull();
        assertThat(Functions.cstr(TS1)).isNotNull();
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource({
        "0.5,                 0.5",
        "-0.5,                -0.5",
        "8,                   8",
        "100,                 100",
        "1234567.5,           1234567.5",
        "8.78787878,          8.78787878",
        "1.23456789012345678, 1.23456789012346"
    })
    void cstr_number_plainWithoutGrouping(double value, String expected) {
        assertThat(Functions.cstr(value)).isEqualTo(expected);
    }

    @Test
    void cstr_numberNullOrInteger_convertsLikeAccess() {
        assertThat(Functions.cstr((Double) null)).isNull();
        assertThat(Functions.cstr((Integer) null)).isNull();
        assertThat(Functions.cstr(1234567)).isEqualTo("1234567");
    }

    // the test runs with Locale.US, so the comma groups thousands like in an English Access
    @ParameterizedTest(name = "[{index}] \"{0}\" => {1}")
    @CsvSource({
        "1.5,     1.5",
        "' 2.5 ', 2.5",
        "'1,234.5', 1234.5",
        "-3,      -3"
    })
    void cdbl_text_parsedInDefaultLocale(String value, double expected) throws UcanaccessSQLException {
        assertThat(Functions.cdbl(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] \"{0}\" => {1}")
    @CsvSource({
        "2.5, 2",
        "3.5, 4",
        "-7,  -7"
    })
    void cint_text_roundedHalfEven(String value, short expected) throws UcanaccessSQLException {
        assertThat(Functions.cint(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] \"{0}\"")
    @ValueSource(strings = {"1.5x", "abc", ""})
    void cdbl_noNumber_throws(String value) {
        assertThatThrownBy(() -> Functions.cdbl(value)).isInstanceOf(InvalidFunctionParameterException.class);
        assertThatThrownBy(() -> Functions.cint(value)).isInstanceOf(InvalidFunctionParameterException.class);
    }

    @Test
    void cdblCint_nullText_returnsNull() throws UcanaccessSQLException {
        assertThat(Functions.cdbl((String) null)).isNull();
        assertThat(Functions.cint((String) null)).isNull();
    }

    @Test
    void csng_number_sevenSignificantDigits() {
        assertThat(Functions.csng(1.23456789)).isEqualTo(1.234568);
        assertThat(Functions.csng(123456789.0)).isEqualTo(123456800.0);
        assertThat(Functions.csng(null)).isNull();
    }

    @ParameterizedTest(name = "[{index}] CByte({0}) => {1}")
    @CsvSource({
        "12.5,  12",
        "13.5,  14",
        "0,     0",
        "255.4, 255"
    })
    void cbyte_number_roundedHalfEven(double value, short expected) {
        assertThat(Functions.cbyte(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] CByte({0})")
    @ValueSource(doubles = {256, 255.5, -1, -0.6})
    void cbyte_outOfRange_throws(double value) {
        assertThatThrownBy(() -> Functions.cbyte(value)).isInstanceOf(UcanaccessRuntimeException.class);
    }

    @ParameterizedTest(name = "[{index}] FormatNumber({0}, {1}, {2}, {3}, {4}) => {5}")
    @CsvSource(delimiter = '|', value = {
        "1234.5   | -1 | -2 | -2 | -2 | 1,234.50",
        "1234.5   |  0 | -2 | -2 | -2 | 1,235",
        "0.5      |  2 |  0 | -2 | -2 | .50",
        "-1234.5  |  1 | -1 | -1 |  0 | (1234.5)",
        "-1234.5  |  1 | -1 |  0 | -1 | -1,234.5",
        "2.345    |  2 | -2 | -2 | -2 | 2.35",
        "-0.004   |  2 | -2 | -2 | -2 | 0.00"
    })
    void formatNumber_options_formattedLikeAccess(double value, int digits, int leadingDigit, int parens, int group, String expected) {
        assertThat(Functions.formatNumber(value, digits, leadingDigit, parens, group)).isEqualTo(expected);
    }

    @Test
    void formatCurrencyPercent_number_formattedInDefaultLocale() {
        assertThat(Functions.formatCurrency(1234.5)).isEqualTo("$1,234.50");
        assertThat(Functions.formatCurrency(1234.5, 0)).isEqualTo("$1,235");
        assertThat(Functions.formatPercent(0.25)).isEqualTo("25.00%");
        assertThat(Functions.formatPercent(0.255, 1)).isEqualTo("25.5%");
        assertThat(Functions.formatPercent(-0.5, 0, -1, -1)).isEqualTo("(50%)");
        assertThat(Functions.formatNumber(null)).isNull();
        assertThatThrownBy(() -> Functions.formatNumber(1.0, -3)).isInstanceOf(UcanaccessRuntimeException.class);
    }

    @Test
    void formatCurrencyPercent_germanLocale_likeGermanAccess() {
        Locale.setDefault(Locale.GERMANY);
        try {
            assertThat(Functions.formatNumber(1234.5)).isEqualTo("1.234,50");
            assertThat(Functions.formatCurrency(1234.5)).isEqualTo("1.234,50 \u20ac");
            assertThat(Functions.formatPercent(0.255, 1)).isEqualTo("25,5%");
        } finally {
            Locale.setDefault(Locale.US);
        }
    }

    @ParameterizedTest(name = "[{index}] FormatDateTime({0}, {1}) => {2}")
    @CsvSource(delimiter = '|', value = {
        "2026-10-29 13:14:15 | 0 | 10/29/2026 1:14:15 PM",
        "2026-10-29 00:00:00 | 0 | 10/29/2026",
        "1899-12-30 13:14:15 | 0 | 1:14:15 PM",
        "2026-10-29 13:14:15 | 1 | Thursday, October 29, 2026",
        "2026-10-29 13:14:15 | 2 | 10/29/2026",
        "2026-10-29 13:14:15 | 3 | 1:14:15 PM",
        "2026-10-29 13:14:15 | 4 | 13:14"
    })
    void formatDateTime_namedFormat_formattedLikeAccess(String date, int format, String expected) throws UcanaccessSQLException {
        assertThat(Functions.formatDateTime(Timestamp.valueOf(date), format)).isEqualTo(expected);
    }

    @Test
    void formatDateTime_nullOrInvalidFormat_nullOrThrows() throws UcanaccessSQLException {
        assertThat(Functions.formatDateTime(null)).isNull();
        assertThatThrownBy(() -> Functions.formatDateTime(TS1, 5)).isInstanceOf(InvalidFunctionParameterException.class);
    }

    @Test
    void lenFormatRound_numberWithoutSecondArgument_likeAccess() throws UcanaccessSQLException {
        assertThat(Functions.len(123.0)).isEqualTo(3);
        assertThat(Functions.len(1.5)).isEqualTo(3);
        assertThat(Functions.len((Double) null)).isNull();
        assertThat(Functions.format(1.5)).isEqualTo("1.5");
        assertThat(Functions.format((Double) null)).isNull();
        assertThat(Functions.format("abc")).isEqualTo("abc");
        assertThat(Functions.format((Timestamp) null)).isNull();
        assertThat(Functions.round(2.5)).isEqualTo(2.0);
        assertThat(Functions.round(-3.5)).isEqualTo(-4.0);
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource({
        "2.5,  2",
        "3.5,  4",
        "-2.5, -2",
        "4.51, 5",
        "8.49, 8"
    })
    void cint_cLng_halfRoundsToEven(double value, int expected) {
        assertThat(Functions.cint(value)).isEqualTo((short) expected);
        assertThat(Functions.clng(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0}, {1} => {2}")
    @CsvSource({
        "2.5,       0, 2.0",
        "3.5,       0, 4.0",
        "-2.5,      0, -2.0",
        "0.125,     2, 0.12",
        "0.375,     2, 0.38",
        "1234.5678, 2, 1234.57",
        "-1.005,    1, -1.0"
    })
    void round_decimals_halfRoundsToEven(double value, double decimals, double expected) {
        assertThat(Functions.round(value, decimals)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource({
        "1.23455,   1.2346",
        "1.23465,   1.2346",
        "1.00015,   1.0002",
        "-1.23465,  -1.2346",
        "123.45678, 123.4568"
    })
    void ccur_fourDecimals_halfRoundsToEven(BigDecimal value, BigDecimal expected) {
        assertThat(Functions.ccur(value)).isEqualTo(expected);
        assertThat(Functions.ccur(value.doubleValue())).isEqualTo(expected);
    }

    @Test
    void ccur_null_returnsNull() {
        assertThat(Functions.ccur((BigDecimal) null)).isNull();
        assertThat(Functions.ccur((Double) null)).isNull();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource({
        "yyyy, 2027-01-15 10:30:45",
        "q,    2026-04-15 10:30:45",
        "m,    2026-02-15 10:30:45",
        "y,    2026-01-16 10:30:45",
        "d,    2026-01-16 10:30:45",
        "w,    2026-01-16 10:30:45",
        "ww,   2026-01-22 10:30:45",
        "h,    2026-01-15 11:30:45",
        "n,    2026-01-15 10:31:45",
        "s,    2026-01-15 10:30:46"
    })
    void dateAdd_interval_addsOneUnit(String interval, String expected) throws UcanaccessSQLException {
        assertThat(Functions.dateAdd(interval, 1, TS1)).isEqualTo(Timestamp.valueOf(expected));
        assertThat(Functions.dateAdd(interval, 1, DT1)).isEqualTo(Timestamp.valueOf(expected));
    }

    @Test
    void dateAdd_nullOrUnknownInterval_returnsNullOrThrows() throws UcanaccessSQLException {
        assertThat(Functions.dateAdd(null, 1, TS1)).isNull();
        assertThat(Functions.dateAdd("d", 1, (Timestamp) null)).isNull();
        assertThat(Functions.dateAdd("d", 1, (String) null)).isNull();
        assertThat(Functions.dateAdd("d", 1, java.sql.Date.valueOf("2026-01-15"))).isEqualTo(java.sql.Date.valueOf("2026-01-16"));
        assertThatThrownBy(() -> Functions.dateAdd("x", 1, TS1)).isInstanceOf(UcanaccessSQLException.class);
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource({
        "yyyy, 1",
        "q,    4",
        "m,    13",
        "y,    401",
        "d,    401",
        "w,    57",
        "ww,   57",
        "h,    9626",
        "n,    577575",
        "s,    34654500"
    })
    void dateDiff_interval_signedDifference(String interval, int expected) throws UcanaccessSQLException {
        assertThat(Functions.dateDiff(interval, TS1, TS2)).isEqualTo(expected);
        assertThat(Functions.dateDiff(interval, TS2, TS1)).isEqualTo(-expected);
        assertThat(Functions.dateDiff(interval, DT1, DT2)).isEqualTo(expected);
        assertThat(Functions.dateDiff(interval, DT1, TS2)).isEqualTo(expected);
        assertThat(Functions.dateDiff(interval, TS1, DT2)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0}, {1} => {2}")
    @CsvSource({
        "2026-03-31 00:00:00, 2026-04-01 00:00:00,  1",
        "2026-04-01 00:00:00, 2026-03-31 00:00:00, -1",
        "2026-01-01 00:00:00, 2026-03-31 00:00:00,  0",
        "2026-12-31 00:00:00, 2027-01-01 00:00:00,  1"
    })
    void dateDiff_quarterBoundary_countsBoundariesCrossed(String dt1, String dt2, int expected) throws UcanaccessSQLException {
        assertThat(Functions.dateDiff("q", Timestamp.valueOf(dt1), Timestamp.valueOf(dt2))).isEqualTo(expected);
    }

    @Test
    void dateDiff_nullOrUnknownInterval_returnsNullOrThrows() throws UcanaccessSQLException {
        assertThat(Functions.dateDiff(null, TS1, TS2)).isNull();
        assertThat(Functions.dateDiff("d", (Timestamp) null, TS2)).isNull();
        assertThat(Functions.dateDiff("d", TS1, (Timestamp) null)).isNull();
        assertThatThrownBy(() -> Functions.dateDiff("x", TS1, TS2)).isInstanceOf(UcanaccessSQLException.class);
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource({
        "yyyy, 2026",
        "q,    1",
        "d,    15",
        "y,    15",
        "m,    1",
        "ww,   3",
        "w,    5",
        "h,    10",
        "n,    30",
        "s,    45"
    })
    void datePart_interval_returnsPart(String interval, int expected) throws UcanaccessSQLException {
        assertThat(Functions.datePart(interval, TS1)).isEqualTo(expected);
        assertThat(Functions.datePart(interval, DT1)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] firstDayOfWeek={0}, firstWeekOfYear={1} => {2}")
    @CsvSource({
        "0, 1, 3",
        "2, 1, 3",
        "1, 2, 2",
        "1, 3, 2"
    })
    void datePart_weekOfYearWithOptions_returnsWeek(int firstDayOfWeek, int firstWeekOfYear, int expected) throws UcanaccessSQLException {
        assertThat(Functions.datePart("ww", TS1, firstDayOfWeek, firstWeekOfYear)).isEqualTo(expected);
        assertThat(Functions.datePart("ww", DT1, firstDayOfWeek, firstWeekOfYear)).isEqualTo(expected);
    }

    @Test
    void datePart_weekdayStartingMonday_thursdayIsFour() throws UcanaccessSQLException {
        assertThat(Functions.datePart("w", DT1, 2)).isEqualTo(4);
    }

    @Test
    void datePart_nullOrUnknownInterval_returnsNullOrThrows() throws UcanaccessSQLException {
        assertThat(Functions.datePart(null, TS1)).isNull();
        assertThat(Functions.datePart("d", (Timestamp) null)).isNull();
        assertThatThrownBy(() -> Functions.datePart("x", TS1)).isInstanceOf(UcanaccessSQLException.class);
    }

    @ParameterizedTest(name = "[{index}] {0}, {1} => {2}")
    @CsvSource({
        "0, yes/no,     No",
        "2, yes/no,     Yes",
        "0, true/false, False",
        "2, true/false, True",
        "0, On/Off,     Off",
        "2, On/Off,     On"
    })
    void format_booleanFormat_returnsText(double value, String format, String expected) throws UcanaccessSQLException {
        assertThat(Functions.format(value, format)).isEqualTo(expected);
    }

    @Test
    void iif_condition_returnsMatchingValue() {
        assertThat(Functions.iif(true, 1, 2)).isEqualTo(1);
        assertThat(Functions.iif(false, 1.5, 2.5)).isEqualTo(2.5);
        assertThat(Functions.iif(true, TS1, TS2)).isEqualTo(TS1);
        assertThat(Functions.iif(true, 1.0, "n")).isEqualTo("1");
        assertThat(Functions.iif(false, 1.5, "n")).isEqualTo("n");
        assertThat(Functions.iif(false, "n", 1.5)).isEqualTo("1.5");
        assertThat(Functions.iif(true, (Double) null, "n")).isNull();
    }

    @Test
    void isNull_nullOrValue_detectsNull() {
        assertThat(Functions.isNull((String) null)).isTrue();
        assertThat(Functions.isNull("a")).isFalse();
        assertThat(Functions.isNull((Timestamp) null)).isTrue();
        assertThat(Functions.isNull(TS1)).isFalse();
        assertThat(Functions.isNull((Double) null)).isTrue();
        assertThat(Functions.isNull(1.0)).isFalse();
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource(delimiter = '|', value = {
        "$12      | true",
        "+12      | true",
        "-12.5    | true",
        ".5       | true",
        "',5'     | false",
        "1,234.5  | true",
        "abc      | false"
    })
    void isNumeric_string_detectsNumber(String value, boolean expected) {
        assertThat(Functions.isNumeric(value)).isEqualTo(expected);
    }

    @Test
    void left_nullOrLength_returnsPrefix() {
        assertThat(Functions.left(null, 1)).isNull();
        assertThat(Functions.left("abc", -1)).isNull();
        assertThat(Functions.left("abc", 5)).isEqualTo("abc");
        assertThat(Functions.left("abc", 2)).isEqualTo("ab");
        assertThat(Functions.mid("abc", 2)).isEqualTo("bc");
        assertThat(Functions.mid(null, 2)).isNull();
    }

    @Test
    void monthName_number_returnsName() throws UcanaccessSQLException {
        assertThat(Functions.monthName(2)).isEqualTo("February");
        assertThat(Functions.monthName(2, true)).isEqualTo("Feb");
        assertThatThrownBy(() -> Functions.monthName(0, false)).isInstanceOf(UcanaccessSQLException.class);
        assertThatThrownBy(() -> Functions.monthName(13, false)).isInstanceOf(UcanaccessSQLException.class);
    }

    @Test
    void stringFunctions_nullOrValue_buildText() {
        assertThat(Functions.string(3, null)).isNull();
        assertThat(Functions.string(3, "")).isEmpty();
        assertThat(Functions.string(3, "xyz")).isEqualTo("xxx");
        assertThat(Functions.space(3)).isEqualTo("   ");
        assertThat(Functions.space(null)).isEmpty();
        assertThat(Functions.strReverse(null)).isNull();
        assertThat(Functions.strReverse("abc")).isEqualTo("cba");
    }

    @ParameterizedTest(name = "[{index}] {0}, {1} => {2}")
    @CsvSource({
        ",    1, ",
        "aBc, 1, ABC",
        "aBc, 2, abc",
        "aBc, 4, aBc",
        "hello WORLD, 3, Hello World",
        "'a  b', 3, 'A  B'",
        "o'neil-SMITH x, 3, O'neil-smith X"
    })
    void strConv_conversion_changesCase(String value, int conversion, String expected) {
        assertThat(Functions.strConv(value, conversion)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0}, {1}, {2} => {3}")
    @CsvSource({
        "abc, abc, -1, 0",
        "abc, ABC, -1, 0",
        "abc, ABC,  0, 1",
        "abc, ABC,  1, 0",
        "abc, ABC,  2, 0",
        "a,   c,    0, -1",
        "c,   A,    1, 1",
        "\u00e4, b,  1, -1",
        "\u00e4, a,  1, 1",
        "\u00c4, \u00e4, 1, 0",
        "a-b, ab,   1, 1",
        "\u00df, ss, 1, 0",
        ",    a,    0, ",
        "a,   ,     1, "
    })
    void strComp_compareType_minusOneZeroOrOne(String value1, String value2, int compare, Integer expected) throws UcanaccessSQLException {
        assertThat(Functions.strComp(value1, value2, compare)).isEqualTo(expected);
    }

    @Test
    void strComp_noCompareTypeOrInvalid_ignoresCaseOrThrows() throws UcanaccessSQLException {
        assertThat(Functions.strComp("abc", "ABC")).isZero();
        assertThat(Functions.strComp("a", "B")).isEqualTo(-1);
        assertThatThrownBy(() -> Functions.strComp("a", "b", 5)).isInstanceOf(UcanaccessSQLException.class);
    }

    @ParameterizedTest(name = "[{index}] {0}, {1}, {2}, {3} => {4}")
    @CsvSource({
        "1, Found on the Net, the, -1, 10",
        "1, Found on the Net, THE, -1, 10",
        "1, Found on the Net, THE,  0, 0",
        "1, Found on the Net, f,    1, 1",
        "1, Found on the Net, f,    0, 0",
        "2, Found on the Net, x,   -1, 0",
        "4, Found on the Net, n,   -1, 4",
        "5, Found on the Net, n,   -1, 8",
        "17, Found on the Net, t,  -1, 0",
        "3, abc,              '',  -1, 3",
        "5, abc,              '',  -1, 5",
        "1, '',               '',  -1, 0",
        "1, '',               a,   -1, 0",
        "1, ,                 a,   -1, ",
        "1, abc,              ,    -1, "
    })
    void instr_startAndCompare_positionOrZero(int start, String text, String search, int compare, Integer expected) {
        assertThat(Functions.instr(start, text, search, compare)).isEqualTo(expected);
    }

    @Test
    void instr_startBelowOne_throws() {
        assertThatThrownBy(() -> Functions.instr(0, "abc", "a")).isInstanceOf(UcanaccessRuntimeException.class);
    }

    @ParameterizedTest(name = "[{index}] {0}, {1}, {2}, {3} => {4}")
    @CsvSource({
        "alphabet, a,  -1, -1, 5",
        "alphabet, A,  -1, -1, 5",
        "alphabet, A,  -1,  0, 0",
        "alphabet, a,   4, -1, 1",
        "alphabet, a,   5, -1, 5",
        "alphabet, et,  8, -1, 7",
        "alphabet, et,  7, -1, 0",
        "alphabet, a,   9, -1, 0",
        "alphabet, x,  -1, -1, 0",
        "alphabet, '',  3, -1, 3",
        ",         a,  -1, -1, ",
        "alphabet, ,   -1, -1, "
    })
    void instrrev_startAndCompare_positionOrZero(String text, String search, int start, int compare, Integer expected) {
        assertThat(Functions.instrrev(text, search, start, compare)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(ints = {0, -2})
    void instrrev_invalidStart_throws(int start) {
        assertThatThrownBy(() -> Functions.instrrev("abc", "a", start)).isInstanceOf(UcanaccessRuntimeException.class);
    }

    @Test
    void nz_nullOrValue_returnsDefaultOrValue() {
        assertThat(Functions.nz((String) null)).isEmpty();
        assertThat(Functions.nz("a")).isEqualTo("a");
        assertThat(Functions.nz((Double) null)).isEqualTo(0.0);
        assertThat(Functions.nz(1.5)).isEqualTo(1.5);
        assertThat(Functions.nz((Integer) null)).isZero();
        assertThat(Functions.nz(3)).isEqualTo(3);
        assertThat(Functions.nz((BigDecimal) null)).isEqualTo(BigDecimal.ZERO);
        assertThat(Functions.nz(BigDecimal.TEN)).isEqualTo(BigDecimal.TEN);
        assertThat(Functions.nz(null, BigDecimal.ONE)).isEqualTo(BigDecimal.ONE);
        assertThat(Functions.nz(BigDecimal.TEN, BigDecimal.ONE)).isEqualTo(BigDecimal.TEN);
        assertThat(Functions.nz(null, TS2)).isEqualTo(TS2);
        assertThat(Functions.nz(TS1, TS2)).isEqualTo(TS1);
    }

    // the fraction of a negative date value is a positive time, as Access stores negative dates
    @ParameterizedTest(name = "[{index}] CDate({0}) => {1}")
    @CsvSource({
        "45000,   2023-03-15 00:00:00",
        "45000.5, 2023-03-15 12:00:00",
        "0,       1899-12-30 00:00:00",
        "-1.25,   1899-12-29 06:00:00"
    })
    void cdate_number_daysSinceBaseDate(double value, String expected) {
        assertThat(Functions.cdate(value)).isEqualTo(Timestamp.valueOf(expected));
    }

    @Test
    void numericFunctions_value_computeResult() {
        assertThat(Functions.sgn(2.5)).isEqualTo((short) 1);
        assertThat(Functions.sgn(0.0)).isEqualTo((short) 0);
        assertThat(Functions.sgn(null)).isNull();
        assertThat(Functions.sign(-2.5)).isEqualTo((short) -1);
        assertThat(Functions.val((BigDecimal) null)).isNull();
        assertThat(Functions.val(new BigDecimal("12.5"))).isEqualTo(12.5);
        assertThat(Functions.fix(null)).isNull();
        assertThat(Functions.fix(2.7)).isEqualTo(2.0);
        assertThat(Functions.fix(-2.7)).isEqualTo(-2.0);
        assertThat(Functions.fix(-2147483649.5)).isEqualTo(-2147483649.0);
        assertThat(Functions.mint(null)).isNull();
        assertThat(Functions.mint(-2.5)).isEqualTo(-3L);
        assertThat(Functions.mint(2147483648.5)).isEqualTo(2147483648L);
    }

    @ParameterizedTest(name = "[{index}] {0} => \"{1}\"")
    @CsvSource(delimiter = '|', quoteCharacter = '"', value = {
        "0       | \" 0\"",
        "1       | \" 1\"",
        "1.5     | \" 1.5\"",
        "-0.5    | -.5",
        "0.25    | \" .25\"",
        "-1110.5 | -1110.5"
    })
    void str_number_signOrSpaceWithoutLeadingZero(double value, String expected) {
        assertThat(Functions.str(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0} => {1}")
    @CsvSource({
        "255,   FF",
        "-1,    FFFFFFFF",
        "255.5, 100",
        "254.5, FE",
        "4294967296, 100000000"
    })
    void hex_number_upperCaseRounded(double value, String expected) {
        assertThat(Functions.hex(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] \"{0}\" => {1}")
    @CsvSource(delimiter = '|', quoteCharacter = '"', value = {
        "&HFF       | 255",
        "&hFFFF     | -1",
        "&HFFFFFFFF | -1",
        "&H10000    | 65536",
        "&O17       | 15",
        "&17        | 15",
        "&H         | 0",
        "1e3        | 1000",
        "1d3        | 1000",
        "-.5E1      | -5",
        "1e         | 1",
        "\" 1 2 3\"   | 123",
        "1.5.3      | 1.5",
        "+          | 0"
    })
    void val_radixExponentOrSeparators_parsesLikeAccess(String value, double expected) {
        assertThat(Functions.val(value)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] \"{0}\" => {1}")
    @CsvSource({
        "1,      true",
        "-1,     true",
        "' 0.5', true",
        "0,      false",
        "True,   true",
        "FALSE,  false"
    })
    void cbool_string_numberOrTrueFalse(String value, boolean expected) {
        assertThat(Functions.cbool(value)).isEqualTo(expected);
    }

    @Test
    void cbool_invalidString_throws() {
        assertThatThrownBy(() -> Functions.cbool("Wahr")).isInstanceOf(UcanaccessRuntimeException.class);
    }

    @ParameterizedTest(name = "[{index}] Replace({0}, {1}, {2}, {3}, {4}, {5}) => {6}")
    @CsvSource({
        "abcb, b,  x,   1, -1, -1, axcx",
        "aBc,  b,  x,   1, -1, -1, axc",
        "aBc,  b,  x,   1, -1,  0, aBc",
        "abcb, b,  x,   1,  1, -1, axcb",
        "abcb, b,  x,   3, -1, -1, cx",
        "abcb, b,  x,   5, -1, -1, ''",
        "abcb, '', x,   1, -1, -1, abcb",
        "abcb, b,  '',  1, -1, -1, ac",
        "aaa,  aa, b,   1, -1, -1, ba",
        "abc,  b,  x,   1,  0, -1, abc",
        ",     b,  x,   1, -1, -1, "
    })
    void replace_startCountAndCompare_replacesLikeAccess(String expression, String find, String replacement, int start, int count,
        int compare, String expected) throws UcanaccessSQLException {
        assertThat(Functions.replace(expression, find, replacement, start, count, compare)).isEqualTo(expected);
    }

    @Test
    void replace_invalidStart_throws() {
        assertThatThrownBy(() -> Functions.replace("abc", "b", "x", 0)).isInstanceOf(InvalidFunctionParameterException.class);
    }

    @ParameterizedTest(name = "[{index}] Format(\"{0}\", \"{1}\") => {2}")
    @CsvSource({
        "abc, >, ABC",
        "ABC, <, abc",
        "a1,  >, A1"
    })
    void format_caseSymbol_changesCase(String value, String format, String expected) throws UcanaccessSQLException {
        assertThat(Functions.format(value, format)).isEqualTo(expected);
    }

    @Test
    void pmt_futureValue_includedInPayment() {
        assertThat(Functions.pmt(0.01, 12, 1000, 500)).isEqualTo(Functions.pmt(0.01, 12, 1000, 500, 0))
            .isCloseTo(-128.2731830175125, within(1e-9));
    }

    @Test
    void rate_loan_returnsInterestRate() {
        assertThat(Functions.rate(12, -88.85, 1000, 0, 0)).isCloseTo(0.01, within(1e-4));
        assertThat(Functions.rate(12, -88.85, 1000, 0, 0, 0.05)).isCloseTo(0.01, within(1e-4));
        assertThat(Functions.rate(12, -88.85, 1000, 0, 0, 0)).isCloseTo(0.01, within(1e-4));
    }

    @Test
    void rnd_argument_newRepeatedOrFixedNumber() {
        double r = Functions.rnd(1.0);
        assertThat(r).isBetween(0.0, 1.0);
        assertThat(Functions.rnd(0.0)).isEqualTo(r);
        assertThat(Functions.rnd(-2.0)).isEqualTo(Functions.rnd(-1.0));
        assertThat(Functions.rnd()).isLessThan(1.0);
    }

    @ParameterizedTest(name = "[{index}] Partition({0}, {1}, 100, {2}) => \"{3}\"")
    @CsvSource(delimiter = '|', quoteCharacter = '"', value = {
        "5   | 1  | 10 | \"  1: 10\"",
        "15  | 1  | 10 | \" 11: 20\"",
        "100 | 1  | 10 | \" 91:100\"",
        "120 | 1  | 10 | \"101:   \"",
        "5   | 11 | 10 | \"   : 10\"",
        "3   | 0  | 5  | \"  0:  4\"",
        "-1  | 0  | 5  | \"   : -1\"",
        "0   | 1  | 10 | \"   :  0\""
    })
    void partition_number_returnsRange(double number, double start, double interval, String expected) throws UcanaccessSQLException {
        assertThat(Functions.partition(number, start, 100, interval)).isEqualTo(expected);
    }

    @Test
    void partition_null_returnsNull() throws UcanaccessSQLException {
        assertThat(Functions.partition(null, 1, 100, 10)).isNull();
    }

    // an interval of 0 used to loop forever
    @ParameterizedTest(name = "[{index}] Partition(5, {0}, {1}, {2})")
    @CsvSource({
        "0,   100, 0",
        "0,   100, 0.4",
        "10,  0,   5",
        "10,  10,  5",
        "-10, 10,  5"
    })
    void partition_invalidRangeOrInterval_throws(double start, double stop, double interval) {
        assertThatThrownBy(() -> Functions.partition(5.0, start, stop, interval)).isInstanceOf(InvalidFunctionParameterException.class);
    }

    @Test
    void formulaToNumeric_text_parsesNumber() {
        assertThat(Functions.formulaToNumeric((String) null, "DOUBLE")).isNull();
        assertThat(Functions.formulaToNumeric("1,234.5", "DOUBLE")).isEqualTo(1234.5);
        assertThat(Functions.formulaToNumeric("1,234.5", "LONG")).isEqualTo(1235.0);
    }

    @Test
    void timeSerial_hourMinuteSecond_timeOnBaseDate() {
        assertThat(Functions.timeSerial(13, 14, 15)).isEqualTo(Timestamp.valueOf(LocalDateTime.of(1899, 12, 30, 13, 14, 15)));
        assertThat(Functions.timeSerial(null, 14, 15)).isNull();
    }

    @ParameterizedTest(name = "[{index}] TimeValue(\"{0}\") => {1}")
    @CsvSource({
        "13:14:15,               1899-12-30 13:14:15",
        "' 1:14 PM ',            1899-12-30 13:14:00",
        "12:30 am,               1899-12-30 00:30:00",
        "12:00 PM,               1899-12-30 12:00:00",
        "8:05,                   1899-12-30 08:05:00",
        "2026-10-29 13:14:15,    1899-12-30 13:14:15",
        "10/29/2026 1:14:15 PM,  1899-12-30 13:14:15"
    })
    void timeValue_text_timeOnBaseDate(String value, String expected) {
        assertThat(Functions.timeValue(value)).isEqualTo(Timestamp.valueOf(expected));
    }

    @ParameterizedTest(name = "[{index}] TimeValue(\"{0}\")")
    @ValueSource(strings = {"25:00", "10:60", "13:00 PM", "abc"})
    void timeValue_invalidText_throws(String value) {
        assertThatThrownBy(() -> Functions.timeValue(value)).isInstanceOf(UcanaccessRuntimeException.class);
    }

    @Test
    void timeValueTimer_dateOrNull_timeOfDay() {
        assertThat(Functions.timeValue(TS1)).isEqualTo(Timestamp.valueOf("1899-12-30 10:30:45"));
        assertThat(Functions.timeValue((Timestamp) null)).isNull();
        assertThat(Functions.timeValue((String) null)).isNull();
        assertThat(Functions.timer()).isBetween(0.0, 86400.0);
    }

    // values out of range carry over like in Access, a negative total keeps the absolute time of day
    @ParameterizedTest(name = "[{index}] TimeSerial({0}, {1}, {2}) => {3}")
    @CsvSource({
        "25,  0,  0, 1899-12-31 01:00:00",
        "0,   90, 0, 1899-12-30 01:30:00",
        "0,   0, -1, 1899-12-30 00:00:01",
        "-25, 0,  0, 1899-12-29 01:00:00"
    })
    void timeSerial_outOfRange_carriesOver(int h, int m, int s, String expected) {
        assertThat(Functions.timeSerial(h, m, s)).isEqualTo(Timestamp.valueOf(expected));
    }

    // Access dates have no time zone, so a daylight saving time change must not affect the result
    @ParameterizedTest(name = "[{index}] {0}, {1} => {2}")
    @CsvSource({
        "2026-03-29, h, 24",
        "2026-03-29, n, 1440",
        "2026-03-29, s, 86400",
        "2026-03-29, d, 1",
        "2026-10-25, h, 24",
        "2026-10-25, s, 86400"
    })
    void dateDiff_acrossDaylightSavingChange_countsWallClockTime(String day, String interval, int expected) throws Exception {
        LocalDateTime start = LocalDate.parse(day).atStartOfDay();
        Integer diff = inTimeZone("Europe/Berlin",
            () -> Functions.dateDiff(interval, Timestamp.valueOf(start), Timestamp.valueOf(start.plusDays(1))));
        assertThat(diff).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"2026-03-29", "2026-10-25"})
    void dateAdd_hoursAcrossDaylightSavingChange_keepsWallClockTime(String day) throws Exception {
        LocalDateTime start = LocalDate.parse(day).atStartOfDay();
        LocalDateTime result = inTimeZone("Europe/Berlin",
            () -> Functions.dateAdd("h", 24, Timestamp.valueOf(start)).toLocalDateTime());
        assertThat(result).isEqualTo(start.plusDays(1));
    }

    @Test
    void formulaToText_noon_formattedWithTime() throws UcanaccessSQLException {
        Timestamp noon = Timestamp.valueOf("2026-01-15 12:00:00");
        assertThat(Functions.formulaToText(noon, "DATETIME")).isEqualTo(Functions.format(noon, "general date"));
    }

    private static <T> T inTimeZone(String zoneId, Callable<T> callable) throws Exception {
        TimeZone defaultZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
            return callable.call();
        } finally {
            TimeZone.setDefault(defaultZone);
        }
    }

}
