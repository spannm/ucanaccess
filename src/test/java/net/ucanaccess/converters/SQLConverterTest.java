package net.ucanaccess.converters;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

/**
 * Unit tests for the {@link net.ucanaccess.converters.SQLConverter} class.
 */
@SuppressWarnings("checkstyle:MethodName")
class SQLConverterTest extends AbstractBaseTest {

    @Nested
    @DisplayName("Translate Access Power Operators (^ to POWER())")
    class TranslateAccessPowerOperatorsTests {

        @ParameterizedTest(name = "{index}: {2}: {0}")
        @CsvSource(
            delimiter = '|',
            nullValues = {"(null)"},
            value = {
                // inputSql | expectedSql | description

                // Basic functionality: integer operands
                "SELECT 2^3 FROM Dual | SELECT POWER(2, 3) FROM Dual | Basic integer operands",
                // Basic functionality: decimal operands
                "SELECT 2.5^3.0 FROM Dual | SELECT POWER(2.5, 3.0) FROM Dual | Basic decimal operands",
                // Functionality with column names
                "SELECT MyColumn^ExponentVal FROM MyTable | SELECT POWER(MyColumn, ExponentVal) FROM MyTable | Column names as operands",
                // Fully qualified column names (Table.Column)
                "SELECT MyTable.MyColumn^AnotherTable.ExpVal FROM MyTable | SELECT POWER(MyTable.MyColumn, AnotherTable.ExpVal) FROM MyTable | Fully qualified column names",
                // Mixed operands: column and number
                "SELECT MyColumn^3.14 FROM MyTable | SELECT POWER(MyColumn, 3.14) FROM MyTable | Mixed operands: column and number (exponent)",
                "SELECT 10^ExpVal FROM MyTable | SELECT POWER(10, ExpVal) FROM MyTable | Mixed operands: column and number (base)",
                // Whitespace variations around the operator
                "SELECT Col1 ^ Col2 FROM T1 | SELECT POWER(Col1, Col2) FROM T1 | Whitespace around operator (spaces)",
                "SELECT Col1^ Col2 FROM T1 | SELECT POWER(Col1, Col2) FROM T1 | Whitespace around operator (space after)",
                "SELECT Col1 ^Col2 FROM T1 | SELECT POWER(Col1, Col2) FROM T1 | Whitespace around operator (space before)",
                "SELECT Col1^Col2 FROM T1 | SELECT POWER(Col1, Col2) FROM T1 | Whitespace around operator (no spaces)",
                // No power operator present
                "SELECT Column1 + Column2 FROM Table1 | SELECT Column1 + Column2 FROM Table1 | No power operator",

                // String literal containing caret - needs careful handling, often better for MethodSource or CsvFileSource if '^' is literal
                // "SELECT 'hello^world' FROM Dual | SELECT 'hello^world' FROM Dual | String literal containing caret - not handled",

                // Multiple power operations in one SQL string
                "SELECT A^B + C^D FROM T | SELECT POWER(A, B) + POWER(C, D) FROM T | Multiple power operations",
                "UPDATE T SET X = Y^Z WHERE P = Q^R | UPDATE T SET X = POWER(Y, Z) WHERE P = POWER(Q, R) | Multiple operations in UPDATE/WHERE",
                // Scientific notation numbers
                "SELECT 1.23e-4^5.67E+2 FROM T | SELECT POWER(1.23e-4, 5.67E+2) FROM T | Scientific notation numbers",
                // Signed numbers
                "SELECT -2^-3 FROM T | SELECT -POWER(2, -3) FROM T | Signed numbers (sign of the base binds weaker than ^)",
                "SELECT +5^+1.5 FROM T | SELECT +POWER(5, +1.5) FROM T | Signed numbers (positive base, positive exponent)",
                // Simple parenthesized expressions (as per regex definition)
                "SELECT (ColA + 1)^(ColB - 2) FROM T | SELECT POWER((ColA + 1), (ColB - 2)) FROM T | Simple parenthesized expressions",
                "SELECT (10 - 5)^ColX FROM T | SELECT POWER((10 - 5), ColX) FROM T | Simple parenthesized expression (base)",
                "SELECT ColY^(2 * 3) FROM T | SELECT POWER(ColY, (2 * 3)) FROM T | Simple parenthesized expression (exponent)",

                // Nested parentheses (not handled by operand regex) - should remain unchanged, needs careful escaping if SQL contains delimiter
                // "SELECT ((1+2) ^ 3) + ((4+5)^6) FROM Dual | SELECT ((1+2) ^ 3) + ((4+5)^6) FROM Dual | Nested parentheses (not handled by operand regex) - should remain unchanged",

                "SELECT (10)^2 FROM Dual | SELECT POWER((10), 2) FROM Dual | Simple numeric in parentheses",

                // Edge cases: Function calls (NOT supported by regex for operands) - needs careful escaping if SQL contains delimiter
                // "SELECT ABS(Value)^2 FROM T | SELECT ABS(Value)^2 FROM T | Function call (base) - not handled",
                // "SELECT 2^ABS(Value) FROM T | SELECT 2^ABS(Value) FROM T | Function call (exponent) - not handled",

                // Edge cases: Subqueries (NOT supported by regex for operands) - needs careful escaping if SQL contains delimiter
                // "SELECT (SELECT X FROM Y)^2 FROM T | SELECT (SELECT X FROM Y)^2 FROM T | Subquery (base) - not handled",

                // parameter markers (?) for use in PreparedStatement
                "SELECT num^? FROM t | SELECT POWER(num, ?) FROM t | Parameter marker 1a",
                "SELECT num ^ ? FROM t | SELECT POWER(num, ?) FROM t | Parameter marker 1b",
                "SELECT ?^num FROM t | SELECT POWER(?, num) FROM t | Parameter marker 2a",
                "SELECT ? ^ num FROM t | SELECT POWER(?, num) FROM t | Parameter marker 2b",
                "SELECT ?^? FROM t | SELECT POWER(?, ?) FROM t | Parameter marker 3a",
                "SELECT ?  ^  ? FROM t | SELECT POWER(?, ?) FROM t | Parameter marker 3b",

                // Test with empty string or null input
                "(null) | (null) | Null input", // "(null)" will be interpreted as Java null
                "'' | '' | Empty string input", // '' represents an empty string
                // Mixed case SQL keywords (regex uses Pattern.CASE_INSENSITIVE)
                "select Num^Exp from Tab order by Num | select POWER(Num, Exp) from Tab order by Num | Mixed case SQL keywords",
                "SeLeCt Col_A^col_B fRoM my_Table WhErE id = 1 | SeLeCt POWER(Col_A, col_B) fRoM my_Table WhErE id = 1 | Mixed case with various operands"
            }
        )
        void testTranslationScenarios(String inputSql, String expectedSql, String description) {
            assertEquals(expectedSql, SQLConverter.translateAccessPowerOperators(inputSql), "Failed for: " + description);
        }

        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            quoteCharacter = '"',
            value = {
                "SELECT 5 Mod 3 FROM t           | SELECT AccessMod(5, 3) FROM t",
                "SELECT a - 5 mod 3 FROM t       | SELECT a - AccessMod(5, 3) FROM t",
                "SELECT Len(x) Mod 2 FROM t      | SELECT AccessMod(Len(x), 2) FROM t",
                "SELECT a Mod b Mod c FROM t     | SELECT AccessMod(AccessMod(a, b), c) FROM t",
                "SELECT 7 \\ 2, (a+1)\\2 FROM t   | SELECT AccessIntDiv(7, 2), AccessIntDiv((a+1), 2) FROM t",
                "SELECT 'a Mod b', modus FROM t  | SELECT 'a Mod b', modus FROM t"
            }
        )
        void translateAccessIntegerOperators_modAndBackslash_functionCalls(String sql, String expected) {
            assertThat(SQLConverter.translateAccessIntegerOperators(sql)).isEqualTo(expected);
        }

        // in Access * and / bind stronger than \, which binds stronger than Mod, and a sign binds weaker than ^
        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            quoteCharacter = '"',
            value = {
                "SELECT a * b Mod c FROM t         | SELECT AccessMod(a * b, c) FROM t",
                "SELECT a Mod b * c FROM t         | SELECT AccessMod(a, b * c) FROM t",
                "SELECT a / 2 \\ b FROM t          | SELECT AccessIntDiv(a / 2, b) FROM t",
                "SELECT a \\ b * c FROM t          | SELECT AccessIntDiv(a, b * c) FROM t",
                "SELECT a Mod b \\ c FROM t        | SELECT AccessMod(a, AccessIntDiv(b, c)) FROM t",
                "SELECT a \\ b Mod c \\ d FROM t   | SELECT AccessMod(AccessIntDiv(a, b), AccessIntDiv(c, d)) FROM t",
                "SELECT a + b * c Mod d - e FROM t | SELECT a + AccessMod(b * c, d) - e FROM t",
                "SELECT x - 7 Mod -2 FROM t        | SELECT x - AccessMod(7, -2) FROM t",
                "SELECT a * -b \\ c FROM t         | SELECT AccessIntDiv(a * -b, c) FROM t",
                "SELECT Len(Trim(x)) Mod 2 FROM t  | SELECT AccessMod(Len(Trim(x)), 2) FROM t",
                "SELECT .5 Mod 2 FROM t            | SELECT AccessMod(.5, 2) FROM t",
                "SELECT * FROM t WHERE (a) Mod 2=1 | SELECT * FROM t WHERE AccessMod((a), 2)=1"
            }
        )
        void translateAccessIntegerOperators_precedence_likeAccess(String sql, String expected) {
            assertThat(SQLConverter.translateAccessIntegerOperators(sql)).isEqualTo(expected);
        }

        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            value = {
                "SELECT a -2^2 FROM t        | SELECT a -POWER(2, 2) FROM t",
                "SELECT -x^2 FROM t          | SELECT -POWER(x, 2) FROM t",
                "SELECT 2^3^2 FROM t         | SELECT POWER(POWER(2, 3), 2) FROM t",
                "SELECT Abs(x)^2 FROM t      | SELECT POWER(Abs(x), 2) FROM t",
                "SELECT ((a+1)*2)^-x FROM t  | SELECT POWER(((a+1)*2), -x) FROM t"
            }
        )
        void translateAccessPowerOperators_precedence_likeAccess(String sql, String expected) {
            assertThat(SQLConverter.translateAccessPowerOperators(sql)).isEqualTo(expected);
        }

        @Test
        void translateAccessPowerOperators_textLiteral_unchanged() {
            assertThat(SQLConverter.translateAccessPowerOperators("SELECT x ^ 2, '2^3' FROM t")).isEqualTo("SELECT POWER(x, 2), '2^3' FROM t");
            assertThat(SQLConverter.translateAccessPowerOperators("SELECT 'it''s ^ 2', a^b FROM t")).isEqualTo("SELECT 'it''s ^ 2', POWER(a, b) FROM t");
        }

    }

    @Nested
    @DisplayName("Convert Access LIKE clauses to Regex patterns")
    class ConvertToRegexMatchesTests {

        @ParameterizedTest(name = "{index}: input=\"{0}\", expected=\"{1}\"")
        @CsvSource(
            delimiter = '|',
            nullValues = {"(null)"},
            value = {
                // inputLikeClause | expectedRegex
                "a*c | a.*c",
                "a_c | a.c",
                "a#c | a\\dc",
                "a*_# | a.*.\\d",

                // escape sequences
                "a[#]c | a#c",
                "a[*]c | a*c",
                "a[_]c | a_c",

                // combinations of escape and standard wildcards
                "a[#]b*c | a#b.*c",
                "a[#]b*c_d | a#b.*c.d",
                "a#b[*]c | a\\db*c",
                "a_*b[#]c | a..*b#c",

                // more complex patterns
                "[!a-z]*c | [^a-z].*c",
                "test[!a-zA-Z]c | test[^a-zA-Z]c",
                // currently unsupported: "'test[\\*|_|#]' | 'test[\\*|_|#]'",

                // null and empty strings
                "(null) | (null)",
                "|",
                "abc | abc"
            }
        )
        void testConvertToRegexMatches(String inputLikeClause, String expectedRegex) {
            // Null-Eingabe wird separat behandelt, da assertEquals() bei null-Werten scheitert
            if (inputLikeClause == null) {
                assertNull(SQLConverter.convertToRegexMatches(null));
            } else {
                assertEquals(expectedRegex, SQLConverter.convertToRegexMatches(inputLikeClause));
            }
        }

        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            value = {
                "SELECT * FROM t WHERE c LIKE 'a*'                     | SELECT * FROM t WHERE  c  like 'a%'",
                "SELECT * FROM t WHERE c NOT LIKE 'a?b'                | SELECT * FROM t WHERE  c  NOT  like 'a_b'",
                "SELECT * FROM t WHERE (c LIKE 'a#')                   | SELECT * FROM t WHERE (REGEXP_MATCHES(c,'a\\d')  )",
                "SELECT * FROM t WHERE c LIKE '[a-c]*' AND d LIKE 'x'  | SELECT * FROM t WHERE REGEXP_MATCHES(c,'[a-c].*')   AND  d  like 'x'"
            }
        )
        void convertSQL_likeCondition_convertedToLikeOrRegexp(String sql, String expected) {
            assertThat(SQLConverter.convertSQL(sql, true).getSql()).isEqualTo(expected);
        }

        // HSQLDB divides integers as integers, Access always as floating point numbers
        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            value = {
                "SELECT a/b FROM t           | SELECT a * CAST(1 AS DOUBLE) / b FROM t",
                "SELECT 'a/b' FROM t         | SELECT 'a/b' FROM t",
                "SELECT a /* x */ FROM t     | SELECT a /* x */ FROM t",
                "SELECT #1/2/2026# FROM t    | SELECT Timestamp('2026-1-2 00:00:00') FROM t"
            }
        )
        void convertSQL_division_floatingPoint(String sql, String expected) {
            assertThat(SQLConverter.convertSQL(sql, true).getSql()).isEqualTo(expected);
        }

        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            value = {
                "SELECT '#13:14:15#' FROM t                  | SELECT '#13:14:15#' FROM t",
                "SELECT 'it''s #1/2/2026#' FROM t            | SELECT 'it''s #1/2/2026#' FROM t",
                "SELECT '#2026-01-02#', #2026-01-02# FROM t  | SELECT '#2026-01-02#', Timestamp0('2026-01-02 00:00:00') FROM t"
            }
        )
        void convertAccessDate_textLiteral_unchanged(String sql, String expected) {
            assertThat(SQLConverter.convertAccessDate(sql)).isEqualTo(expected);
        }

        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            value = {
                "SELECT #2026-01-01 23:59# FROM t      | SELECT Timestamp0('2026-01-01 23:59:00') FROM t",
                "SELECT #1/2/2026 1:14 PM# FROM t      | SELECT (Timestamp0('2026-1-2 1:14:00')+ 12 Hour)  FROM t",
                "SELECT #2026-01-01 23:59:58# FROM t   | SELECT Timestamp0('2026-01-01 23:59:58') FROM t"
            }
        )
        void convertSQL_dateLiteralWithoutSeconds_secondsAdded(String sql, String expected) {
            assertThat(SQLConverter.convertSQL(sql, true).getSql()).isEqualTo(expected);
        }
    }

    @Nested
    @DisplayName("Identifiers and default values")
    class IdentifiersAndDefaultsTests {

        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            value = {
                "SELECT 1abc2, x FROM t | SELECT Z_1abc2, x FROM t",
                "SELECT 1a1a1 FROM t    | SELECT Z_1a1a1 FROM t",
                "SELECT 123 FROM t      | SELECT 123 FROM t",
                "SELECT 1E20 FROM t     | SELECT 1E20 FROM t",
                "SELECT 1e-5, 2E+3 FROM t | SELECT 1e-5, 2E+3 FROM t",
                "SELECT 1E2x FROM t     | SELECT Z_1E2x FROM t"
            }
        )
        void testDigitStartingIdentifiers(String sql, String expected) {
            assertEquals(expected, SQLConverter.convertSQL(sql, true).getSql());
        }

        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            quoteCharacter = '`',
            value = {
                "c TEXT DEFAULT 'abc'      | 'abc'",
                "c TEXT DEFAULT 'it''s'    | 'it''s'",
                "c TEXT DEFAULT \"a\"\"b\" | \"a\"\"b\"",
                "c LONG DEFAULT 42         | 42",
                "c DATETIME DEFAULT Now()  | Now()"
            }
        )
        void testGetDDLDefault(String ddl, String expected) {
            assertEquals(expected, SQLConverter.getDDLDefault(ddl));
        }

        @Test
        void testLargeInputsDoNotOverflowStack() {
            String literal = "'" + "ab''".repeat(5000) + "'";
            assertEquals(literal, SQLConverter.getDDLDefault("c MEMO DEFAULT " + literal));

            String identifier = "1" + "a1".repeat(5000);
            assertEquals("SELECT Z_" + identifier + " FROM t", SQLConverter.convertSQL("SELECT " + identifier + " FROM t", true).getSql());
        }

        @ParameterizedTest(name = "{index}: {0}")
        @CsvSource(
            delimiter = '|',
            value = {
                "SELECT _a, __b1 FROM t    | SELECT Z_a, Z__b1 FROM t",
                "SELECT [x y]. FROM t      | SELECT (x y)FROM t"
            }
        )
        void convertSQL_underscoreIdentifierOrBracketedSubquery_converted(String sql, String expected) {
            assertThat(SQLConverter.convertSQL(sql, true).getSql()).isEqualTo(expected);
        }

        @Test
        void convertSQL_largeInput_noCatastrophicBacktracking() {
            String underscores = "SELECT " + "_".repeat(20000) + " FROM t";
            String longLiteral = "SELECT * FROM t WHERE c = '" + "a".repeat(20000) + "'";
            String longWhitespace = "SELECT * FROM t WHERE c =" + " ".repeat(5000) + "1";
            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                assertThat(SQLConverter.convertSQL(underscores, true).getSql()).isEqualTo(underscores.replace(" _", " Z_"));
                assertThat(SQLConverter.convertSQL(longLiteral, true).getSql()).isEqualTo(longLiteral);
                assertThat(SQLConverter.convertSQL(longWhitespace, true).getSql()).isEqualTo(longWhitespace);
            });
        }
    }

}
