package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import net.ucanaccess.test.util.AddFunctionClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Statement;
import java.util.Locale;

@RunWith(Parameterized.class)
public class AddFunctionTest extends AccessVersionAllTest {

    public AddFunctionTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testAddFunction() throws Exception {
        /*
         * Display the Locale language in effect (assuming that this is the first test in the suite). Unfortunately,
         * `-Duser.language=tr` (for Turkish) can be used for an individual test but does does not seem to affect an
         * entire suite
         */
        getLogger().info("Locale language is {}", Locale.getDefault().getLanguage());
        Thread.sleep(1500);

        Statement st = ucanaccess.createStatement();
        st.executeUpdate("CREATE TABLE gooo (id INTEGER) ");
        st.close();
        st = ucanaccess.createStatement();
        st.execute("INSERT INTO gooo (id )  VALUES(1)");
        ucanaccess.addFunctions(AddFunctionClass.class);
        dumpQueryResult("SELECT pluto('hello',' world ',  now ()) FROM gooo");
        checkQuery("SELECT concat('Hello World, ','Ucanaccess') FROM gooo", "Hello World, Ucanaccess");
        // uc.addFunctions(AddFunctionClass.class);

        dropTable("gooo");
    }

}
