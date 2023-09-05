package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2007Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

@RunWith(Parameterized.class)
public class CorruptedTest extends AccessVersion2007Test {

    private static final ByteArrayOutputStream ERR_CONTENT = new ByteArrayOutputStream();

    @BeforeClass
    public static void setUpStreams() {
        System.setErr(new PrintStream(ERR_CONTENT));
    }

    @AfterClass
    public static void cleanUpStreams() {
        System.setErr(System.err);
    }

    public CorruptedTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/corrupted.accdb"; // Access 2007
    }

    @Test
    public void testCorrupted() {
        getLogger().info("UcanaccessConnection: {}", ucanaccess);
        String err = "WARNING:integrity constraint violation: foreign key no parent; BABY_DADDYBABY table: BABY value: 34"
                + System.lineSeparator()
                + "WARNING:Detected Foreign Key constraint breach, table Baby, record Row[162:1][{ID=2,fk1=34}]: making the table Baby read-only"
                + System.lineSeparator()
                + "WARNING:Detected Not Null constraint breach, table NotNull, record Row[140:0][{ID=1,notnull=<null>,vvv=gg,fk1=34}]: making the table NotNull read-only"
                + System.lineSeparator()
                + "WARNING:integrity constraint violation: foreign key no parent; NOTNULL_DADDYNOTNULL table: NOTNULL value: 34"
                + System.lineSeparator()
                + "WARNING:Detected Foreign Key constraint breach, table NotNull, record Row[140:3][{ID=4,notnull=t,vvv=t,fk1=2}]: making the table NotNull read-only"
                + System.lineSeparator()
                + "WARNING:Detected Unique constraint breach, table UK, record Row[181:1][{ID=2,uk=1}]: making the table UK read-only";
        assertEquals(err, ERR_CONTENT.toString().trim());
    }
}
