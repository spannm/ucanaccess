package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

public abstract class AccessVersionAllTest extends UcanaccessTestBase {

    private static final List<Object[]> VERSIONS = Arrays.asList(
        new Object[] {AccessVersion.V2000},
        new Object[] {AccessVersion.V2003},
        new Object[] {AccessVersion.V2007},
        new Object[] {AccessVersion.V2010},
        new Object[] {AccessVersion.V2016});

    public AccessVersionAllTest(AccessVersion _fileFormat) {
        super(_fileFormat.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> getAllAccessVersions() {
        return VERSIONS;
    }

}
