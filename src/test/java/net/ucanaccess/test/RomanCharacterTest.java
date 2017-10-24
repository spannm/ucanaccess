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

import com.healthmarketscience.jackcess.Database.FileFormat;

public class RomanCharacterTest extends UcanaccessTestBase {
    public RomanCharacterTest() {
        super();
    }

    public RomanCharacterTest(FileFormat accVer) {
        super(accVer);
    }

    @Override
    public String getAccessPath() {
        return "net/ucanaccess/test/resources/noroman.mdb";
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

    }

    public void testNoRomanCharactersInColumnName() throws Exception {
        dump("SELECT * FROM NOROMAN");
        System.out.println("q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß");
        Statement st = null;
        try {
            st = super.ucanaccess.createStatement();
            st.execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß])  VALUES( 'the end','yeeep')");
            st.execute("UPDATE NOROMAN SET [q3¹²³¼½¾ß€Ð×ÝÞðýþäüöß]='NOOOp' WHERE [end]='the end' ");
            checkQuery("SELECT * FROM NOROMAN");
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }
}
