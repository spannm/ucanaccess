package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class GeneratedKeys2Test extends UcanaccessBaseTest {
    private String tableName = "T_Key1";

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE " + tableName + " ( Z GUID PRIMARY KEY, B char(4) )");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testGeneratedKeys(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        try (PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO " + tableName + " (B) VALUES (?)")) {
            ps.setString(1, "");
            ps.execute();
            ResultSet rs1 = ps.getGeneratedKeys();
            rs1.next();
            try (PreparedStatement ps1 = ucanaccess.prepareStatement("Select @@identity ");
                ResultSet rs2 = ps1.executeQuery()) {
                rs2.next();
                assertEquals(rs1.getString(1), rs2.getString(1));
            }
        }
        checkQuery("SELECT * FROM " + tableName);

    }
}
