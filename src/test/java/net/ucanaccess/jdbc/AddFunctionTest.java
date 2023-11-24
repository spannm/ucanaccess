package net.ucanaccess.jdbc;

import net.ucanaccess.converters.AddFunctions;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Locale;

class AddFunctionTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testAddFunction(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        /*
         * Display the Locale language in effect (assuming that this is the first test in the suite). Unfortunately,
         * `-Duser.language=tr` (for Turkish) can be used for an individual test but does does not seem to affect an
         * entire suite
         */
        getLogger().info("Locale language is {}", Locale.getDefault().getLanguage());

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("CREATE TABLE t_add_function (id INTEGER) ");
        }

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_add_function (id) VALUES(1)");
        }

        ucanaccess.addFunctions(AddFunctions.class);
        dumpQueryResult("SELECT pluto('hello', ' world ', NOW()) FROM t_add_function");
        checkQuery("SELECT CONCAT('Hello World, ', 'Ucanaccess') FROM t_add_function", singleRec("Hello World, Ucanaccess"));

        executeStatements("DROP TABLE t_add_function");
    }

}
