package net.ucanaccess.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * Unit tests for the {@link net.ucanaccess.converters.SQLConverter} class.
 */
class SQLConverterTest {

    @Nested
    @DisplayName("Translate Access Power Operators (^ to POWER())")
    static class TranslateAccessPowerOperatorsTests {

        // A static method to provide test arguments for the parameterized test
        private static Stream<Arguments> provideSqlTranslationData() {
            return Stream.of(
                // Basic functionality: integer operands
                arguments("SELECT 2^3 FROM Dual", "SELECT POWER(2, 3) FROM Dual", "Basic integer operands"),
                // Basic functionality: decimal operands
                arguments("SELECT 2.5^3.0 FROM Dual", "SELECT POWER(2.5, 3.0) FROM Dual", "Basic decimal operands"),
                // Functionality with column names
                arguments("SELECT MyColumn^ExponentVal FROM MyTable", "SELECT POWER(MyColumn, ExponentVal) FROM MyTable", "Column names as operands"),
                // Fully qualified column names (Table.Column)
                arguments("SELECT MyTable.MyColumn^AnotherTable.ExpVal FROM MyTable", "SELECT POWER(MyTable.MyColumn, AnotherTable.ExpVal) FROM MyTable", "Fully qualified column names"),
                // Mixed operands: column and number
                arguments("SELECT MyColumn^3.14 FROM MyTable", "SELECT POWER(MyColumn, 3.14) FROM MyTable", "Mixed operands: column and number (exponent)"),
                arguments("SELECT 10^ExpVal FROM MyTable", "SELECT POWER(10, ExpVal) FROM MyTable", "Mixed operands: column and number (base)"),
                // Whitespace variations around the operator
                arguments("SELECT Col1 ^ Col2 FROM T1", "SELECT POWER(Col1, Col2) FROM T1", "Whitespace around operator (spaces)"),
                arguments("SELECT Col1^ Col2 FROM T1", "SELECT POWER(Col1, Col2) FROM T1", "Whitespace around operator (space after)"),
                arguments("SELECT Col1 ^Col2 FROM T1", "SELECT POWER(Col1, Col2) FROM T1", "Whitespace around operator (space before)"),
                arguments("SELECT Col1^Col2 FROM T1", "SELECT POWER(Col1, Col2) FROM T1", "Whitespace around operator (no spaces)"),
                // No power operator present
                arguments("SELECT Column1 + Column2 FROM Table1", "SELECT Column1 + Column2 FROM Table1", "No power operator"),

                //arguments("SELECT 'hello^world' FROM Dual", "SELECT 'hello^world' FROM Dual", "String literal containing caret - not handled"),

                // Multiple power operations in one SQL string
                arguments("SELECT A^B + C^D FROM T", "SELECT POWER(A, B) + POWER(C, D) FROM T", "Multiple power operations"),
                arguments("UPDATE T SET X = Y^Z WHERE P = Q^R", "UPDATE T SET X = POWER(Y, Z) WHERE P = POWER(Q, R)", "Multiple operations in UPDATE/WHERE"),
                // Scientific notation numbers
                arguments("SELECT 1.23e-4^5.67E+2 FROM T", "SELECT POWER(1.23e-4, 5.67E+2) FROM T", "Scientific notation numbers"),
                // Signed numbers
                arguments("SELECT -2^-3 FROM T", "SELECT POWER(-2, -3) FROM T", "Signed numbers (negative base, negative exponent)"),
                arguments("SELECT +5^+1.5 FROM T", "SELECT POWER(+5, +1.5) FROM T", "Signed numbers (positive base, positive exponent)"),
                // Simple parenthesized expressions (as per regex definition)
                arguments("SELECT (ColA + 1)^(ColB - 2) FROM T", "SELECT POWER((ColA + 1), (ColB - 2)) FROM T", "Simple parenthesized expressions"),
                arguments("SELECT (10 - 5)^ColX FROM T", "SELECT POWER((10 - 5), ColX) FROM T", "Simple parenthesized expression (base)"),
                arguments("SELECT ColY^(2 * 3) FROM T", "SELECT POWER(ColY, (2 * 3)) FROM T", "Simple parenthesized expression (exponent)"),

                //arguments("SELECT ((1+2) ^ 3) + ((4+5)^6) FROM Dual", "SELECT ((1+2) ^ 3) + ((4+5)^6) FROM Dual", "Nested parentheses (not handled by operand regex) - should remain unchanged"),

                arguments("SELECT (10)^2 FROM Dual", "SELECT POWER((10), 2) FROM Dual", "Simple numeric in parentheses"),

                // Edge cases: Function calls (NOT supported by regex for operands) - should remain unchanged
                //arguments("SELECT ABS(Value)^2 FROM T", "SELECT ABS(Value)^2 FROM T", "Function call (base) - not handled"),
                //arguments("SELECT 2^ABS(Value) FROM T", "SELECT 2^ABS(Value) FROM T", "Function call (exponent) - not handled"),

                // Edge cases: Subqueries (NOT supported by regex for operands) - should remain unchanged
                //arguments("SELECT (SELECT X FROM Y)^2 FROM T", "SELECT (SELECT X FROM Y)^2 FROM T", "Subquery (base) - not handled"),

                // Test with empty string or null input
                arguments(null, null, "Null input"),
                arguments("", "", "Empty string input"),
                // Mixed case SQL keywords (regex uses Pattern.CASE_INSENSITIVE)
                arguments("select Num^Exp from Tab order by Num", "select POWER(Num, Exp) from Tab order by Num", "Mixed case SQL keywords"),
                arguments("SeLeCt Col_A^col_B fRoM my_Table WhErE id = 1", "SeLeCt POWER(Col_A, col_B) fRoM my_Table WhErE id = 1", "Mixed case with various operands")
            );
        }

        @ParameterizedTest(name = "{2}: {0}")
        @MethodSource("provideSqlTranslationData")
        void testTranslationScenarios(String inputSql, String expectedSql, String description) {
            assertEquals(expectedSql, SQLConverter.translateAccessPowerOperators(inputSql), "Failed for: " + description);
        }
    }

}
