package net.ucanaccess.console;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class MainTest extends AbstractBaseTest {

    @Test
    void testTokenize() throws IOException {
        // normal space as token delimiter
        List<String> tokens = Main.tokenize("export -t License License.csv");
        assertThat(tokens).containsExactly("export", "-t", "License", "License.csv");

        // tab (\t), line-feed (\n) and carriage-return (\r) as token delimiter
        tokens = Main.tokenize("export\t-t\nLicense\rLicense.csv");
        assertThat(tokens).containsExactly("export", "-t", "License", "License.csv");

        // backslash escapes are not interpreted outside of quote
        tokens = Main.tokenize("export -t License c:\\temp\\License.csv");
        assertThat(tokens).containsExactly("export", "-t", "License", "c:\\temp\\License.csv");

        // backslash escapes are interpreted inside quote, so we have to double escape
        tokens = Main.tokenize("export -t License 'c:\\\\temp\\\\License.csv");
        assertThat(tokens).containsExactly("export", "-t", "License", "c:\\temp\\License.csv");

        // octal escapes are not interpreted outside of quote
        tokens = Main.tokenize("export -t License c:\\101");
        assertThat(tokens).containsExactly("export", "-t", "License", "c:\\101");

        // octal escapes are interpreted inside a quote
        tokens = Main.tokenize("export -t License 'c:\\101'");
        assertThat(tokens).containsExactly("export", "-t", "License", "c:A");

        // embedded space inside double quote
        tokens = Main.tokenize("export \"License and Address.csv\"");
        assertThat(tokens).containsExactly("export", "License and Address.csv");

        // embedded space inside single quote
        tokens = Main.tokenize("export 'License and Address.csv'");
        assertThat(tokens).containsExactly("export", "License and Address.csv");

        // single quote and embedded space inside double quote
        tokens = Main.tokenize("export \"License 'and' Address.csv\"");
        assertThat(tokens).containsExactly("export", "License 'and' Address.csv");

        // double quote and embedded space inside single quote
        tokens = Main.tokenize("export 'License \"and\" Address.csv'");
        assertThat(tokens).containsExactly("export", "License \"and\" Address.csv");

        // backslash escaped double quote inside double quote
        tokens = Main.tokenize("export \"\\\"License and Address.csv\\\"\"");
        assertThat(tokens).containsExactly("export", "\"License and Address.csv\"");

        // non-breaking-white-space U+00A0 is treated like a normal character
        tokens = Main.tokenize("export License\u00A0and\u00A0Address.csv");
        assertThat(tokens).containsExactly("export", "License\u00A0and\u00A0Address.csv");

        // any unicode > U+00FF is treated like a normal character, U+202F is NARROW NO-BREAK SPACE
        tokens = Main.tokenize("export License\u202Fand\u202FAddress.csv");
        assertThat(tokens).containsExactly("export", "License\u202Fand\u202FAddress.csv");
    }
}
