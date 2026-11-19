package net.ucanaccess.console;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Tests the {@code export} command of the console {@link Main}.
 */
@SuppressWarnings({"checkstyle:MethodName", "PMD.LinguisticNaming"})
class ExportCommandTest extends UcanaccessBaseTest {

    private Main main;

    /**
     * Creates a database with a single table and a console on top of it.
     */
    @BeforeEach
    void setUp() throws Exception {
        init(AccessVersion.getDefaultAccessVersion());
        executeStatements("CREATE TABLE t_export (id INTEGER, name TEXT)",
            "INSERT INTO t_export (id, name) VALUES (1, 'Grüße')");
        main = new Main(ucanaccess, new BufferedReader(new StringReader("")));
    }

    @Test
    void executeExport_bigQuerySchema_writesCsvAndSchemaToTheirOwnFiles() throws Exception {
        File csv = createTempFileName("export", ".csv");
        File schema = createTempFileName("export", ".schema");

        main.executeExport(String.format("export -t t_export --big_query_schema %s %s", quote(schema), quote(csv)));

        assertThat(Files.readAllLines(csv.toPath(), StandardCharsets.UTF_8)).first().isEqualTo("id;name");
        assertThat(Files.readString(schema.toPath(), StandardCharsets.UTF_8))
            .startsWith("[").contains("{\"name\": \"id\", \"type\": \"int64\", \"mode\": \"nullable\"}");
    }

    @Test
    void executeExport_doubleDash_takesNextTokenAsFileName() throws Exception {
        File csv = createTempFileName("-export", ".csv");

        main.executeExport(String.format("export -t t_export -- %s", quote(csv)));

        assertThat(csv).exists();
    }

    @Test
    void executeExport_nonAsciiText_writesUtf8() throws Exception {
        File csv = createTempFileName("export", ".csv");

        main.executeExport(String.format("export -t t_export %s", quote(csv)));

        assertThat(Files.readAllLines(csv.toPath(), StandardCharsets.UTF_8)).containsExactly("id;name", "1;Grüße");
    }

    /**
     * Quotes the path of the given file for the tokenizer of the export command.
     */
    private static String quote(File file) {
        return '"' + file.getAbsolutePath().replace('\\', '/') + '"';
    }
}
