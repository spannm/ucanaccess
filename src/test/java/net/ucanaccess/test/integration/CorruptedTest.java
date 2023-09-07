package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;

class CorruptedTest extends UcanaccessTestBase {

    private static final ByteArrayOutputStream ERR_CONTENT = new ByteArrayOutputStream();

    @BeforeAll
    static void setUpStreams() {
        System.setErr(new PrintStream(ERR_CONTENT));
    }

    @AfterAll
    static void cleanUpStreams() {
        System.setErr(System.err);
    }

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "corrupted.accdb"; // Access 2007
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testCorrupted(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        getLogger().info("UcanaccessConnection: {}", ucanaccess);
        String nl = System.lineSeparator();
        String err = "WARNING:integrity constraint violation: foreign key no parent; BABY_DADDYBABY table: BABY value: 34" + nl
            + "WARNING:Detected Foreign Key constraint breach, table Baby, record Row[162:1][{ID=2,fk1=34}]: making the table Baby read-only" + nl
            + "WARNING:Detected Not Null constraint breach, table NotNull, record Row[140:0][{ID=1,notnull=<null>,vvv=gg,fk1=34}]: making the table NotNull read-only" + nl
            + "WARNING:integrity constraint violation: foreign key no parent; NOTNULL_DADDYNOTNULL table: NOTNULL value: 34" + nl
            + "WARNING:Detected Foreign Key constraint breach, table NotNull, record Row[140:3][{ID=4,notnull=t,vvv=t,fk1=2}]: making the table NotNull read-only" + nl
            + "WARNING:Detected Unique constraint breach, table UK, record Row[181:1][{ID=2,uk=1}]: making the table UK read-only";
        assertEquals(err, ERR_CONTENT.toString().trim());
    }

}
