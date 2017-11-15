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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class WorkloadTest extends AccessVersionDefaultTest {

    public WorkloadTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE AAAB ( id COUNTER PRIMARY KEY,A LONG , C TEXT,D TEXT) ");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("AAAB");
    }

    @Test
    public void testLoadMany() throws SQLException, IOException, InterruptedException {
        final int nbRecords = 30000;
        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i <= nbRecords; i++) {
            st.execute("INSERT INTO AAAB(id,a,c,d) VALUES (" + i + ",'33','booo','ddddddddddddddddddddd' )");
        }
        ucanaccess.commit();
        long time1 = System.currentTimeMillis();
        getLogger().info("Autoincrement insert performance test, {} records inserted in {} seconds.", nbRecords,
                TimeUnit.MILLISECONDS.toSeconds(time1 - startTime));
        st = ucanaccess.createStatement();
        st.executeUpdate("update aaAB set c='yessssss'&a");
        ucanaccess.commit();
        long time2 = System.currentTimeMillis();
        getLogger().info("Update performance test, all {} table records updated in {} seconds.", nbRecords,
                TimeUnit.MILLISECONDS.toSeconds(time2 - time1));

        st.close();
    }

}
