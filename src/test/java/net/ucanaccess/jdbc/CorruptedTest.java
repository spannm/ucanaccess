package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class CorruptedTest extends UcanaccessBaseFileTest {

    @Test
    void testCorrupted() throws SQLException {
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(errContent)) {

            PrintStream prevSysOut = System.out;
            System.setOut(ps);
            init();
            System.setOut(prevSysOut);
        }

        Pattern pat = Pattern.compile("^[0-9\\-]{10} [0-9\\:]{8}\\.?[0-9]* WARN LoadJet[ \\-]+");
        List<String> actual = Arrays.stream(errContent.toString().split(System.lineSeparator()))
            .map(pat::matcher)
            .filter(Matcher::find)
            .map(m -> m.replaceAll(""))
            .collect(Collectors.toList());

        assertEquals(List.of(
            "integrity constraint violation: foreign key no parent ; BABY_DADDYBABY table: BABY value: 34",
            "Detected Foreign Key constraint breach, table Baby, record Row[162:1][{ID=2,fk1=34}]: making the table Baby read-only",
            "Detected Not Null constraint breach, table NotNull, record Row[140:0][{ID=1,notnull=<null>,vvv=gg,fk1=34}]: making the table NotNull read-only",
            "integrity constraint violation: foreign key no parent ; NOTNULL_DADDYNOTNULL table: NOTNULL value: 34",
            "Detected Foreign Key constraint breach, table NotNull, record Row[140:3][{ID=4,notnull=t,vvv=t,fk1=2}]: making the table NotNull read-only",
            "Detected Unique constraint breach, table UK, record Row[181:1][{ID=2,uk=1}]: making the table UK read-only"), actual);
    }

}
