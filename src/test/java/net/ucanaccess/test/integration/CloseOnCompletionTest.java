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

import java.sql.PreparedStatement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessStatement;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class CloseOnCompletionTest extends AccessVersionAllTest {

    public CloseOnCompletionTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testCloseOnCompletion() throws Exception {

        PreparedStatement st = null;
        st = ucanaccess.prepareStatement("CREATE TABLE pluto1 (id varchar(23)) ");
        ((UcanaccessStatement) st).closeOnCompletion();

        st.execute();
        st.close();

    }

}
