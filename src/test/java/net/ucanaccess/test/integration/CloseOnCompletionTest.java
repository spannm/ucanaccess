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
