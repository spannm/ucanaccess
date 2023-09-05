package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;

@RunWith(Parameterized.class)
public class CloseOnCompletionTest extends AccessVersionAllTest {

    public CloseOnCompletionTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testCloseOnCompletion() throws Exception {

        PreparedStatement st = null;
        st = ucanaccess.prepareStatement("CREATE TABLE pluto1 (id varchar(23)) ");
        st.closeOnCompletion();

        st.execute();
        st.close();

    }

}
