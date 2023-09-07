package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.PreparedStatement;

class CloseOnCompletionTest extends UcanaccessTestBase {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCloseOnCompletion(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        try (PreparedStatement st = ucanaccess.prepareStatement("CREATE TABLE pluto1 (id varchar(23))")) {
            st.closeOnCompletion();

            st.execute();
        }
    }

}
