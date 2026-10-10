package net.ucanaccess.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

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
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Objects;

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
        assertThat(Functions.clng("1,234.5")).isEqualTo(1235);
        assertThat(Functions.cstr("abc")).isEqualTo("abc");
        assertThat(Functions.cstr((Timestamp) null)).isNull();
        assertThat(Functions.cstr((Boolean) null)).isNull();
        assertThat(Functions.cstr(TS1)).isNotNull();
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
        "aBc, 3, aBc",
        "aBc, 4, aBc"
    })
    void strConv_conversion_changesCase(String value, int conversion, String expected) {
        assertThat(Functions.strConv(value, conversion)).isEqualTo(expected);
    }

    @Test
    void strComp_compareType_comparesStrings() throws UcanaccessSQLException {
        assertThat(Functions.strComp("abc", "abc")).isZero();
        assertThat(Functions.strComp("abc", "ABC", 0)).isPositive();
        assertThat(Functions.strComp("abc", "ABC", 1)).isZero();
        assertThatThrownBy(() -> Functions.strComp("a", "b", 5)).isInstanceOf(UcanaccessSQLException.class);
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
    }

    @Test
    void numericFunctions_value_computeResult() {
        assertThat(Functions.sgn(2.5)).isEqualTo((short) 1);
        assertThat(Functions.sgn(0)).isEqualTo((short) 0);
        assertThat(Functions.sign(-2.5)).isEqualTo((short) -1);
        assertThat(Functions.val((BigDecimal) null)).isNull();
        assertThat(Functions.val(new BigDecimal("12.5"))).isEqualTo(12.5);
        assertThat(Functions.fix(null)).isNull();
        assertThat(Functions.fix(2.7)).isEqualTo(2.0);
        assertThat(Functions.fix(-2.7)).isEqualTo(-2.0);
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
    void partition_number_returnsRange(double number, double start, double interval, String expected) {
        assertThat(Functions.partition(number, start, 100, interval)).isEqualTo(expected);
    }

    @Test
    void partition_null_returnsNull() {
        assertThat(Functions.partition(null, 1, 100, 10)).isNull();
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
    }

}
