/*
Copyright (c) 2017 Brian Park.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package net.ucanaccess.console;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import net.ucanaccess.test.util.AbstractTestBase;

public class MainTest extends AbstractTestBase {

    @Test
    public void testTokenize() throws IOException {
        // normal space as token delimiter
        List<String> tokens = Main.tokenize("export -t License License.csv");
        assertListEquals(tokens, "export", "-t", "License", "License.csv");

        // tab (\t), line-feed (\n) and carriage-return (\r) as token delimiter
        tokens = Main.tokenize("export\t-t\nLicense\rLicense.csv");
        assertListEquals(tokens, "export", "-t", "License", "License.csv");

        // backslash escapes are not interpreted outside of quote
        tokens = Main.tokenize("export -t License c:\\temp\\License.csv");
        assertListEquals(tokens, "export", "-t", "License", "c:\\temp\\License.csv");

        // backslash escapes are interpreted inside quote, so we have to double escape
        tokens = Main.tokenize("export -t License 'c:\\\\temp\\\\License.csv");
        assertListEquals(tokens, "export", "-t", "License", "c:\\temp\\License.csv");

        // octal escapes are not interpreted outside of quote
        tokens = Main.tokenize("export -t License c:\\101");
        assertListEquals(tokens, "export", "-t", "License", "c:\\101");

        // octal escapes are interpreted inside a quote
        tokens = Main.tokenize("export -t License 'c:\\101'");
        assertListEquals(tokens, "export", "-t", "License", "c:A");

        // embedded space inside double quote
        tokens = Main.tokenize("export \"License and Address.csv\"");
        assertListEquals(tokens, "export", "License and Address.csv");

        // embedded space inside single quote
        tokens = Main.tokenize("export 'License and Address.csv'");
        assertListEquals(tokens, "export", "License and Address.csv");

        // single quote and embedded space inside double quote
        tokens = Main.tokenize("export \"License 'and' Address.csv\"");
        assertListEquals(tokens, "export", "License 'and' Address.csv");

        // double quote and embedded space inside single quote
        tokens = Main.tokenize("export 'License \"and\" Address.csv'");
        assertListEquals(tokens, "export", "License \"and\" Address.csv");

        // backslash escaped double quote inside double quote
        tokens = Main.tokenize("export \"\\\"License and Address.csv\\\"\"");
        assertListEquals(tokens, "export", "\"License and Address.csv\"");

        // non-breaking-white-space U+00A0 is treated like a normal character
        tokens = Main.tokenize("export License\u00A0and\u00A0Address.csv");
        assertListEquals(tokens, "export", "License\u00A0and\u00A0Address.csv");

        // any unicode > U+00FF is treated like a normal character, U+202F is NARROW NO-BREAK SPACE
        tokens = Main.tokenize("export License\u202Fand\u202FAddress.csv");
        assertListEquals(tokens, "export", "License\u202Fand\u202FAddress.csv");
    }
}
