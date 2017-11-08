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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class ConcatNullsTrueTest extends AccessVersionDefaultTest {

    public ConcatNullsTrueTest(AccessVersion _accessVersion) {
        super(_accessVersion);
        // By default, any null value will cause the function to return null.
        // If the property is set false, then NULL values are replaced with empty strings.
        // see: http://hsqldb.org/doc/guide/builtinfunctions-chapt.html
        appendToJdbcURL(";concatnulls=true");
    }

    @Override
    public String getAccessPath() {
        return "testdbs/badDB.accdb";
    }

    @Test
    public void testConcat() throws Exception {
        checkQuery("SELECT 'aa2'& null FROM dual", new Object[][] { { null } });
    }

}
