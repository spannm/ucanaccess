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

import java.lang.reflect.Method;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class PhysicalRollbackTest extends AccessVersionDefaultTest {

    public PhysicalRollbackTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        // this db does not exist!
        return getClass().getSimpleName() + fileFormat.getFileExtension();
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE T4 (id LONG, descr VARCHAR(400))");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("T4");
    }

    @Test
    public void testCommit() throws Exception {
        ucanaccess.setAutoCommit(false);

        Method mth = UcanaccessConnection.class.getDeclaredMethod("setTestRollback", new Class[] { boolean.class });
        mth.setAccessible(true);
        mth.invoke(ucanaccess, new Object[] { Boolean.TRUE });
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO T4 (id,descr) VALUES( 6666554,  'nel mezzo del cammin di nostra vita')");
        st.execute("INSERT INTO T4 (id,descr) VALUES( 77666554, 'nel mezzo del cammin di nostra vita')");
        st.execute("UPDATE T4 SET ID=0 where id=77666554");

        st.execute("INSERT INTO T4 (id,descr) VALUES( 4,'nel mezzo del cammin di nostra vita')");

        st.execute("DELETE FROM T4 WHERE id=4");
        Exception ex = null;
        try {
            ucanaccess.commit();
        } catch (Exception _ex) {
            ex = _ex;
        }
        assertNotNull(ex);
        assertContains(ex.getMessage(), getClass().getSimpleName());

        ucanaccess = getUcanaccessConnection();
        dumpQueryResult("SELECT * FROM T4");

        assertTrue(getCount("SELECT COUNT(*) FROM T4", true) == 0);
    }

}
