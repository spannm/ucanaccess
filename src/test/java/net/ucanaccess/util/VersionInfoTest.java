package net.ucanaccess.util;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class VersionInfoTest extends AbstractBaseTest {

    private Package     mockPackage;

    private VersionInfo versionInfoSpy;

    @BeforeEach
    void setUp() {
        mockPackage = mock(Package.class);
        VersionInfo.clearCache();
        versionInfoSpy = spy(new VersionInfo());
    }

    @AfterEach
    void tearDown() {
        VersionInfo.clearCache();
    }

    @ParameterizedTest(name = "''{0}''")
    @CsvSource(
        delimiter = ',',
        nullValues = {"(null)"},
        value = {
            "1.2.3-SNAPSHOT, 1, 2",
            "2.5.0-beta, 2, 5",
            "3.1.4, 3, 1",
            "4.5.6.7, 4, 5",
            "alpha.beta, 0, 0",
            "1., 1, 0",
            "1, 1, 0",
            "(null), 0, 0"
        }
    )
    void testVersionParsing(String version, int major, int minor) {
        // arrange
        when(mockPackage.getImplementationVersion()).thenReturn(version);
        if (version == null) {
            when(versionInfoSpy.readFromManifest()).thenReturn(null);
            when(versionInfoSpy.readFromMavenPom()).thenReturn(null);
        }

        // act
        VersionInfo result = versionInfoSpy.findVersion(mockPackage);

        // assert
        assertEquals("null".equals(version) ? null : version, result.getVersion());
        assertEquals(major, result.getMajorVersion());
        assertEquals(minor, result.getMinorVersion());
    }

    @Test
    void testFindVersionFromPackage() {
        // arrange
        String expectedVersion = "1.2.3-SNAPSHOT";
        when(mockPackage.getImplementationVersion()).thenReturn(expectedVersion);

        // act
        VersionInfo result = versionInfoSpy.findVersion(mockPackage);

        // assert
        assertEquals(expectedVersion, result.getVersion());
        assertEquals(1, result.getMajorVersion());
        assertEquals(2, result.getMinorVersion());
        verify(versionInfoSpy, times(0)).readFromManifest();
        verify(versionInfoSpy, times(0)).readFromMavenPom();
    }

    @Test
    void testFindVersionFromManifestWhenPackageFails() {
        String expectedVersion = "2.5.0-beta";
        doReturn(expectedVersion).when(versionInfoSpy).readFromManifest();

        VersionInfo result = versionInfoSpy.findVersion(null);

        assertEquals(expectedVersion, result.getVersion());
        assertEquals(2, result.getMajorVersion());
        assertEquals(5, result.getMinorVersion());
        verify(versionInfoSpy, times(1)).readFromManifest();
        verify(versionInfoSpy, times(0)).readFromMavenPom();
    }

    @Test
    void testFindVersionFromMavenPomWhenManifestFails() {
        String expectedVersion = "3.1.4";
        doReturn(null).when(versionInfoSpy).readFromManifest();
        doReturn(expectedVersion).when(versionInfoSpy).readFromMavenPom();

        VersionInfo result = versionInfoSpy.findVersion(null);

        assertEquals(expectedVersion, result.getVersion());
        assertEquals(3, result.getMajorVersion());
        assertEquals(1, result.getMinorVersion());
        verify(versionInfoSpy, times(1)).readFromManifest();
        verify(versionInfoSpy, times(1)).readFromMavenPom();
    }

    @Test
    void testNoVersionFound() {
        doReturn(null).when(versionInfoSpy).readFromManifest();
        doReturn(null).when(versionInfoSpy).readFromMavenPom();

        VersionInfo result = versionInfoSpy.findVersion(null);

        assertNull(result.getVersion());
        assertEquals(0, result.getMajorVersion());
        assertEquals(0, result.getMinorVersion());
    }

    @Test
    void testUnparsableVersionString() {
        String unparsableVersion = "alpha.beta";
        when(mockPackage.getImplementationVersion()).thenReturn(unparsableVersion);

        VersionInfo result = versionInfoSpy.findVersion(mockPackage);

        assertEquals(unparsableVersion, result.getVersion());
        assertEquals(0, result.getMajorVersion());
        assertEquals(0, result.getMinorVersion());
    }

}
