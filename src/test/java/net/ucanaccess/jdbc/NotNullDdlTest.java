package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class NotNullDdlTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void confirmNotNullColumnUsingJet(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        // future-proofing in case default file version changes
        assertEquals(getFileFormat().name(), "V2003");

        String mdbPath = ucanaccess.getDbIO().getFile().getAbsolutePath();

        UcanaccessStatement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE t_nnd (id LONG PRIMARY KEY, txt_required TEXT(50) NOT NULL)");
        ucanaccess.close();

        File vbsFile = createTempFileName(getClass().getSimpleName(), ".vbs");
        vbsFile.deleteOnExit();
        Files.write(vbsFile.toPath(), List.of(
            "Set conn = CreateObject(\"ADODB.Connection\")",
            "conn.Open \"DRIVER={Microsoft Access Driver (*.mdb)};DBQ=" + mdbPath + '\"',
            "conn.Execute \"INSERT INTO t_nnd (id) VALUES (1)\"",
            "conn.Close"), StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        if (!System.getProperty("os.name").startsWith("Windows")) {
            getLogger().log(Level.WARNING, "Not Windows, aborting test");
            return;
        }

        String cscriptPath = String.join("\\",
            System.getenv("SystemRoot"),
            System.getProperty("sun.arch.data.model").equals("64") ? "SYSWOW64" : "SYSTEM32", "CSCRIPT.EXE");

        String[] command = new String[] {"\"" + cscriptPath + "\"", "\"" + vbsFile.getAbsolutePath() + "\""};
        Process proc = Runtime.getRuntime().exec(command);
        proc.waitFor(15, TimeUnit.SECONDS);

        assertThat(proc.exitValue()).isZero();

        try (BufferedReader output = new BufferedReader(new InputStreamReader(proc.getErrorStream()))) {
            String stderr = output.lines().collect(Collectors.joining(System.lineSeparator()));

            if (stderr.isEmpty()) {
                fail("The VBScript should have thrown an error, but it did not");
            }
            assertThat(stderr)
                .withFailMessage("The VBScript threw an unexpected error")
                .contains("t_nnd.txt_required");
        }

    }

}
