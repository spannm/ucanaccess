package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.FROM;
import static net.ucanaccess.type.SqlConstants.SELECT;

import net.ucanaccess.converters.AddFunctions;
import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.Sql;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.System.Logger.Level;
import java.util.Locale;

class AddFunctionTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testAddFunction(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        /*
         * Display the Locale language in effect (assuming that this is the first test in the suite). Unfortunately,
         * `-Duser.language=tr` (for Turkish) can be used for an individual test but does does not seem to affect an
         * entire suite
         */
        getLogger().log(Level.INFO, "Locale language is {0}", Locale.getDefault().getLanguage());

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("CREATE TABLE t_add_function (id INTEGER) ");
        }

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_add_function (id) VALUES(1)");
        }

        ucanaccess.addFunctions(AddFunctions.class);
        dumpQueryResult(Sql.of(SELECT, "pluto('hello', ' world ', NOW())", FROM, "t_add_function"));
        checkQuery(Sql.of(SELECT, "CONCAT('Hello World, ', 'Ucanaccess')", FROM, "t_add_function"), singleRec("Hello World, Ucanaccess"));

        executeStatements("DROP TABLE t_add_function");
    }

}
