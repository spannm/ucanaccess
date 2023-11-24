package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class BatchTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE Tb (id LONG, name TEXT, age LONG)",
            "INSERT INTO Tb VALUES(1, 'Sophia', 33)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBatch(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.addBatch("UPDATE Tb SET [name]='ccc'");
            st.addBatch("UPDATE Tb SET age=95");
            st.executeBatch();
            checkQuery("SELECT * FROM tb");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBatchPS(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        try (PreparedStatement st = ucanaccess.prepareStatement("UPDATE Tb SET [name]=?,age=? ")) {
            st.setString(1, "ciao");
            st.setInt(2, 23);
            st.addBatch();
            st.setString(1, "ciao1");
            st.setInt(2, 43);
            st.addBatch();
            st.executeBatch();
            checkQuery("SELECT * FROM tb");
        }
    }

}
