package net.ucanaccess.converters;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Unit tests for the {@link net.ucanaccess.converters.SQLConverter} class.
 */
class SQLConverterTest extends AbstractBaseTest {

    @Nested
    @DisplayName("Translate Access Power Operators (^ to POWER())")
    static class TranslateAccessPowerOperatorsTests {

        @ParameterizedTest(name = "{2}: {0}")
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
                "SELECT -2^-3 FROM T | SELECT POWER(-2, -3) FROM T | Signed numbers (negative base, negative exponent)",
                "SELECT +5^+1.5 FROM T | SELECT POWER(+5, +1.5) FROM T | Signed numbers (positive base, positive exponent)",
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

    }

}
