/*
Copyright (c) 2012 Marco Amadei.

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
package net.ucanaccess.test.integration;

import java.sql.Statement;
import java.util.Locale;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import net.ucanaccess.test.util.AddFunctionClass;

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
        getLogger().info("Locale language is: {}", Locale.getDefault().getLanguage());
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
