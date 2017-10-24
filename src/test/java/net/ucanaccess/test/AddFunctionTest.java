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
package net.ucanaccess.test;

import java.sql.Statement;
import java.util.Locale;

import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class AddFunctionTest extends UcanaccessTestBase {

    public AddFunctionTest() {
        super();
    }

    public AddFunctionTest(FileFormat accVer) {
        super(accVer);
    }

    public void testAddFunction() throws Exception {
        /*
         * Display the Locale language in effect (assuming that this is the first test in the suite). Unfortunately,
         * `-Duser.language=tr` (for Turkish) can be used for an individual test but does does not seem to affect an
         * entire suite
         */
        System.out.printf("%nLocale language is \"%s\"%n", Locale.getDefault().getLanguage());
        Thread.sleep(1500);

        Statement st = super.ucanaccess.createStatement();
        st.executeUpdate("CREATE TABLE gooo (id INTEGER) ");
        st.close();
        st = super.ucanaccess.createStatement();
        st.execute("INSERT INTO gooo (id )  VALUES(1)");
        UcanaccessConnection uc = (UcanaccessConnection) super.ucanaccess;
        uc.addFunctions(AddFunctionClass.class);
        super.dump("select pluto('hello',' world ',  now ()) from gooo");
        checkQuery("select concat('Hello World, ','Ucanaccess') from gooo", "Hello World, Ucanaccess");
        // uc.addFunctions(AddFunctionClass.class);
    }

}
