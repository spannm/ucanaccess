package net.ucanaccess.about;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

@SuppressWarnings("checkstyle:MethodName")
class ThisLibTest extends AbstractBaseTest {

    private static final String NL = System.lineSeparator();

    @Test
    void buildInfo_mainAndSectionAttributes_printsNonBlankValues() {
        Manifest manifest = new Manifest();
        Attributes main = manifest.getMainAttributes();
        main.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        main.putValue("Project-Name", "UCanAccess");
        main.putValue("Implementation-Version", " 5.1.9 ");
        main.putValue("Project-Description", "JDBC driver");
        main.putValue("Implementation-Vendor", "   ");
        main.putValue("X-BasePOM-Git-Commit-Id", "abc123");
        main.putValue("Project-Url", "https://example.org");

        Attributes section = new Attributes();
        section.putValue("Git-Branch", "master");
        manifest.getEntries().put("net/ucanaccess/", section);

        assertThat(ThisLib.buildInfo(manifest))
            .startsWith(NL + "UCanAccess v5.1.9" + NL + "JDBC driver" + NL + NL)
            .contains(String.format("%-15s: %s", "Git Commit", "abc123") + NL)
            .contains(String.format("%-15s: %s", "Git Branch", "master") + NL)
            .contains(String.format("%-15s: %s", "Homepage", "https://example.org") + NL)
            .doesNotContain("Vendor", "Build JDK")
            .endsWith("not intended for direct CLI execution." + NL);
    }

    @Test
    void buildInfo_noManifest_printsDefaults() {
        assertThat(ThisLib.buildInfo(null))
            .startsWith(NL + "Java Library vunknown" + NL + NL)
            .doesNotContain("Git Commit", "Homepage");
    }

    @Test
    void main_outsideJar_printsNothing() {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(bos, true, StandardCharsets.UTF_8));
            ThisLib.main(new String[0]);
        } finally {
            System.setOut(out);
        }
        assertThat(bos.toString(StandardCharsets.UTF_8)).isEmpty();
    }

}
