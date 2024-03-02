package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.SQLException;

class DataSourceTest extends UcanaccessBaseTest {

    @Test
    void setNewDatabaseVersionBad() {
        UcanaccessDataSource uds = new UcanaccessDataSource();
        assertThatThrownBy(() -> uds.setNewDatabaseVersion("V200?"))
            .isInstanceOf(UcanaccessRuntimeException.class);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ValueSource(strings = {"V2003", "v2003", "V2016"})
    @NullSource
    void setNewDatabaseVersionGood(String _ver) {
        UcanaccessDataSource uds = new UcanaccessDataSource();
        uds.setNewDatabaseVersion(_ver);
        assertThat(uds.getNewDatabaseVersion()).isEqualToIgnoringCase(_ver);
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
        uds.setNewDatabaseVersion(AccessVersion.V2003);
        uds.setImmediatelyReleaseResources(true); // so we can delete it immediately after close

        try (Connection conn = uds.getConnection()) {
            assertThat(fileMdb).exists();
            getLogger().log(Level.INFO, "DataSource connection successfully created, file {0}", uds.getAccessPath());
        }

        Boolean irrEffective = uds.getImmediatelyReleaseResources();
        // Note that a property is returned as null if we haven't explicitly set it in the DataSource
        irrEffective = irrEffective != null && irrEffective;
        if (irrEffective) {
            assertTrue(fileMdb.delete());
            assertThat(fileMdb).doesNotExist();
        } else {
            getLogger().log(Level.INFO, "(Test database remains on disk)");
        }
    }
}
