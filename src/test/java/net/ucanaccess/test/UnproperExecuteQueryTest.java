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
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class UnproperExecuteQueryTest extends UcanaccessTestBase {
    public UnproperExecuteQueryTest() {
        super();
    }

    public UnproperExecuteQueryTest(FileFormat accVer) {
        super(accVer);
    }

    @Override
    public String getAccessPath() {
        return "net/ucanaccess/test/resources/noroman.mdb";
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß])  VALUES( 'the end','yeeep')");
        execute("UPDATE NOROMAN SET [ENd]='BLeah'");

        execute("delete from NOROMAN");

    }

    private void execute(String s) throws SQLException {
        Statement st = super.ucanaccess.createStatement();
        try {

            st.executeQuery(s);
            throw new RuntimeException("not reacheable here");
        } catch (Exception e) {
            // e.printStackTrace();
            System.out.println(e.getMessage());
        }
        st.execute(s);
    }

    public void testOk() throws Exception {
        System.out.println("ok");
    }
}
