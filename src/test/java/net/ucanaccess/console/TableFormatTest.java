package net.ucanaccess.console;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import net.ucanaccess.console.Main.TableFormat;
import net.ucanaccess.console.Main.TableFormat.Border;
import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class TableFormatTest extends AbstractBaseTest {

    @Test
    void output_unicodeBorder_drawsBoxDrawingCharacters() throws SQLException {
        String table = format(Border.UNICODE, -1, false, new Object[] {1, "Alpha"}, new Object[] {22, null});

        assertThat(table).isEqualTo(String.join("\n",
            "",
            "  ┌────┬───────┐",
            "  │ ID │ NAME  │",
            "  ╞════╪═══════╡",
            "  │  1 │ Alpha │",
            "  │ 22 │       │",
            "  └────┴───────┘",
            "  (2 rows)",
            ""));
    }

    @Test
    void output_asciiBorder_drawsPlusMinusPipe() throws SQLException {
        String table = format(Border.ASCII, -1, false, new Object[] {1, "Alpha"});

        assertThat(table).isEqualTo(String.join("\n",
            "",
            "  +----+-------+",
            "  | ID | NAME  |",
            "  +====+=======+",
            "  |  1 | Alpha |",
            "  +----+-------+",
            "  (1 row)",
            ""));
    }

    @Test
    void output_noRows_printsHeaderOnly() throws SQLException {
        String table = format(Border.ASCII, -1, true);

        assertThat(table).isEqualTo(String.join("\n",
            "",
            "  +----+------+",
            "  | ID | NAME |",
            "  +----+------+",
            "  (0 rows)",
            ""));
    }

    @Test
    void output_hideEmptyColumns_namesThemBelowTable() throws SQLException {
        String table = format(Border.ASCII, -1, true, new Object[] {1, null}, new Object[] {2, ""});

        assertThat(table).isEqualTo(String.join("\n",
            "",
            "  +----+",
            "  | ID |",
            "  +====+",
            "  |  1 |",
            "  |  2 |",
            "  +----+",
            "  (2 rows, 1 empty column hidden: NAME)",
            ""));
    }

    @Test
    void output_allColumnsEmpty_hidesNone() throws SQLException {
        String table = format(Border.ASCII, -1, true, new Object[] {null, null});

        assertThat(table).contains("| ID | NAME |").endsWith("  (1 row)\n");
    }

    @Test
    void output_tooWide_printsOneBlockPerRow() throws SQLException {
        String table = format(Border.UNICODE, -1, false, 10, new Object[] {1, "大和"}, new Object[] {22, null});

        assertThat(table).isEqualTo(String.join("\n",
            "",
            "  ┌─ Row 1 ┬──────┐",
            "  │ ID     │ 1    │",
            "  │ NAME   │ 大和 │",
            "  ├─ Row 2 ┼──────┤",
            "  │ ID     │ 22   │",
            "  │ NAME   │      │",
            "  └────────┴──────┘",
            "  (2 rows)",
            ""));
    }

    @Test
    void output_tooWideAscii_labelsRowsInLine() throws SQLException {
        String table = format(Border.ASCII, -1, true, 5, new Object[] {1, null});

        assertThat(table).isEqualTo(String.join("\n",
            "",
            "  +- Row 1 +---+",
            "  | ID     | 1 |",
            "  +--------+---+",
            "  (1 row, 1 empty column hidden: NAME)",
            ""));
    }

    @Test
    void output_tooWideWithoutRows_listsColumns() throws SQLException {
        String table = format(Border.ASCII, -1, false, 10);

        assertThat(table).isEqualTo("\n  (0 rows, columns: ID, NAME)\n");
    }

    @Test
    void output_maxRows_stopsReading() throws SQLException {
        String table = format(Border.ASCII, 1, false, new Object[] {1, "a"}, new Object[] {2, "b"});

        assertThat(table).contains("|  1 |").doesNotContain("|  2 |").endsWith("  (1 row)\n");
    }

    @Test
    void output_wideAndControlCharacters_keepsColumnsAligned() throws SQLException {
        String table = format(Border.UNICODE, -1, false,
            new Object[] {1, "中文测试"},
            new Object[] {2, "日本第一 Nihon Daiichi"},
            new Object[] {3, "敏子 勝 大和 まさと"},
            new Object[] {4, "line1\nline2\u2028line3\tend"},
            new Object[] {5, "é"},
            new Object[] {6, "x".repeat(60)});

        // the first line is empty, the last one the row count
        List<String> lines = Arrays.asList(table.split("\n"));
        assertThat(lines.subList(1, lines.size() - 1)).hasSize(10)
            .allSatisfy(line -> assertThat(TableFormat.displayWidth(line)).isEqualTo(61));
        assertThat(table).contains("│ 中文测试 ").contains("line1 line2 line3 end")
            .contains("│ 日本第一 Nihon Daiichi ")
            .contains("│ 敏子 勝 大和 まさと ");
    }

    @ParameterizedTest(name = "[{index}] {0} -> {1}")
    @CsvSource(delimiter = '|', value = {
        "日本語 | 6",
        "ひらがな | 8",
        "カタカナ | 8",
        "ｶﾀｶﾅ | 4",
        "ｶﾞｷﾞ | 4",
        "「東京」。 | 10",
        "Tokyo東京 | 9",
        "日本第一 | 8",
        "大和 | 4",
        "やまと | 6",
        "まさと | 6",
        "ﾏｻﾄ | 3",
        "敏子 | 4",
        "としこ | 6",
        "勝 | 2",
        "まさる | 6",
        "abc | 3",
        "中文 | 4",
        "한글 | 4",
        "ＡＢ | 4",
        "é | 1",
        "😀 | 2",
        "𠀋 | 2",
        "äöüß | 4",
        "〿 | 1",
        "1⃣ | 1",
        "a‍b | 2"
    })
    void displayWidth_variousScripts_countsConsoleColumns(String str, int expected) {
        assertThat(TableFormat.displayWidth(str)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] ''{0}'' {1} {2} -> ''{3}''")
    @CsvSource(delimiter = '|', ignoreLeadingAndTrailingWhitespace = false, value = {
        "abc|5|false|abc  ",
        "abc|5|true|  abc",
        "abcdef|4|false|abcd",
        "日本第一|7|false|日本第 ",
        "ﾔﾏﾄ ﾏｻﾄ|5|false|ﾔﾏﾄ ﾏ",
        "大和|3|true| 大",
        "敏子 勝|6|false|敏子  ",
        "中文测|5|false|中文 ",
        "中文测|5|true| 中文"
    })
    void fit_widthAndAlignment_padsOrCuts(String str, int width, boolean alignRight, String expected) {
        assertThat(TableFormat.fit(str, width, alignRight)).isEqualTo(expected);
    }

    @Test
    void forCharset_encodableOrNot_choosesBorder() {
        assertThat(Border.forCharset(StandardCharsets.UTF_8)).isEqualTo(Border.UNICODE);
        assertThat(Border.forCharset(StandardCharsets.ISO_8859_1)).isEqualTo(Border.ASCII);
        assertThat(Border.forCharset(StandardCharsets.US_ASCII)).isEqualTo(Border.ASCII);
        // a charset that can only decode
        assertThat(Border.forCharset(Charset.forName("ISO-2022-CN"))).isEqualTo(Border.ASCII);
    }

    /**
     * Formats a result set of an integer column ID and a text column NAME with the given rows.
     */
    private static String format(Border border, int maxRows, boolean hideEmptyColumns, Object[]... rows) throws SQLException {
        return format(border, maxRows, hideEmptyColumns, 200, rows);
    }

    /**
     * Formats a result set of an integer column ID and a text column NAME with the given rows and maximum table width.
     */
    private static String format(Border border, int maxRows, boolean hideEmptyColumns, int maxTableWidth, Object[]... rows)
        throws SQLException {
        ResultSetMetaData meta = mock(ResultSetMetaData.class);
        when(meta.getColumnCount()).thenReturn(2);
        when(meta.getColumnLabel(1)).thenReturn("ID");
        when(meta.getColumnLabel(2)).thenReturn("NAME");
        when(meta.getColumnType(1)).thenReturn(Types.INTEGER);
        when(meta.getColumnType(2)).thenReturn(Types.VARCHAR);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(meta);
        int[] current = {-1};
        when(resultSet.next()).thenAnswer(inv -> ++current[0] < rows.length);
        when(resultSet.getObject(1)).thenAnswer(inv -> rows[current[0]][0]);
        when(resultSet.getObject(2)).thenAnswer(inv -> rows[current[0]][1]);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
            new TableFormat(resultSet, maxRows, border).hideEmptyColumns(hideEmptyColumns).maxTableWidth(maxTableWidth).output(ps);
        }
        return baos.toString(StandardCharsets.UTF_8).replace(System.lineSeparator(), "\n");
    }
}
