package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class CorruptedTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "corrupted.accdb"; // Access 2007
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testCorrupted(AccessVersion _accessVersion) throws SQLException {
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(errContent)) {

            System.setOut(out);
            init(_accessVersion);
            System.setOut(System.err);

            List<String> actual = Arrays.stream(errContent.toString().split(System.lineSeparator()))
                .map(l -> l.replaceAll("^[0-9\\-]{10} [0-9\\:]{8}\\.?[0-9]* WARN LoadJet[ \\-]+", ""))
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

}
