package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.util.UcanaccessRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

class DataSourceTest extends UcanaccessBaseTest {

    @Test
    void setNewDatabaseVersionBad() {
        UcanaccessDataSource uds = new UcanaccessDataSource();
        assertThatThrownBy(() -> uds.setNewDatabaseVersion("V200?"))
            .isInstanceOf(UcanaccessRuntimeException.class);
    }

    @Test
    void setNewDatabaseVersionGood() {
        UcanaccessDataSource uds = new UcanaccessDataSource();
        String ver = "V2003";
        uds.setNewDatabaseVersion(ver);
        assertEquals(ver, uds.getNewDatabaseVersion());
    }

    @Test
    void setLobScaleBad() {
        UcanaccessDataSource uds = new UcanaccessDataSource();
        assertThatThrownBy(() -> uds.setLobScale(3))
            .isInstanceOf(UcanaccessRuntimeException.class);
    }

    @Test
    void setLobScaleGood() {
        UcanaccessDataSource uds = new UcanaccessDataSource();
        Integer val = 4;
        uds.setLobScale(val);
        assertEquals(val, uds.getLobScale());
    }

    @Test
    void createNewDatabase() throws SQLException {
        File fileMdb = createTempFileName("ucaDataSourceTest", ".mdb");
        assertThat(fileMdb).doesNotExist();

        UcanaccessDataSource uds = new UcanaccessDataSource();
        uds.setAccessPath(fileMdb.getAbsolutePath());
        uds.setNewDatabaseVersion("V2003");
        uds.setImmediatelyReleaseResources(true); // so we can delete it immediately after close

        try (Connection conn = uds.getConnection()) {
            assertThat(fileMdb).exists();
            getLogger().info("DataSource connection successfully created, file {}", uds.getAccessPath());
        }

        Boolean irrEffective = uds.getImmediatelyReleaseResources();
        // Note that a property is returned as null if we haven't explicitly set it in the DataSource
        irrEffective = irrEffective != null && irrEffective;
        if (irrEffective) {
            assertTrue(fileMdb.delete());
            assertThat(fileMdb).doesNotExist();
        } else {
            getLogger().info("(Test database remains on disk)");
        }
    }
}
