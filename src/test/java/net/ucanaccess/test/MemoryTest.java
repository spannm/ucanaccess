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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import com.healthmarketscience.jackcess.Database.FileFormat;

public class MemoryTest extends UcanaccessTestBase {

    public MemoryTest() {
        super();
    }

    public MemoryTest(FileFormat accVer) {
        super(accVer);
    }

    @Override
    protected void setUp() throws Exception {

        System.out.println("Thread.activeCount0 " + Thread.activeCount());

        setInactivityTimeout(1);
        super.setUp();
        executeCreateTable("CREATE TABLE memm( id LONG PRIMARY KEY,A LONG , C TEXT,D TEXT) ");

    }

    public void testMemory() throws SQLException, IOException, InterruptedException {
        super.ucanaccess.setAutoCommit(false);
        Statement st = null;
        try {
            st = super.ucanaccess.createStatement();

            System.out.println("total memory 0=" + Runtime.getRuntime().totalMemory());
            System.out.println("free memory 0=" + Runtime.getRuntime().freeMemory());
            for (int i = 0; i <= 100000; i++) {
                st.execute("INSERT INTO memm(id,a,c,d) VALUES (" + i
                        + ",'33','booddddddddddddddddddddddddddddddo','dddddddddddddddddsssssssssssssssdddd' )");
            }
            super.ucanaccess.commit();
            super.ucanaccess.close();
            super.ucanaccess = null;
            long occ = Runtime.getRuntime().freeMemory();
            int ac = Thread.activeCount();
            System.out.println("Thread.activeCount() " + Thread.activeCount());
            System.out.println("total memory 1=" + Runtime.getRuntime().totalMemory());
            System.out.println("free memory 1=" + occ);
            Thread.sleep(61000);
            System.out.println("Thread.activeCount() diff" + (Thread.activeCount() - ac));
            System.out.println("total memory 2=" + Runtime.getRuntime().totalMemory());
            System.out.println("free memory 2=" + Runtime.getRuntime().freeMemory());
            System.out.println("free memory diff =" + (Runtime.getRuntime().freeMemory() - occ));
            // super.ucanaccess=super.getUcanaccessConnection();
            super.ucanaccess = super.getUcanaccessConnection();

            dump("select * from memm limit 10");
            System.out.println("Thread.activeCount() diff" + (Thread.activeCount() - ac));

        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

}
