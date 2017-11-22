package net.ucanaccess.test.util;

import java.util.Arrays;
import java.util.List;

import org.junit.runners.Parameterized;

public abstract class AccessVersionAllTest extends UcanaccessTestBase {

    private static final List<Object[]> VERSIONS = Arrays.asList(
            new Object[] { AccessVersion.V2000 },
            new Object[] { AccessVersion.V2003 },
            new Object[] { AccessVersion.V2007 },
            new Object[] { AccessVersion.V2010 });

    @Parameterized.Parameters(name="{index}: {0}")
    public static Iterable<Object[]> getAllAccessVersions() {
        return VERSIONS;
    }

    public AccessVersionAllTest(AccessVersion _fileFormat) {
        super(_fileFormat.getFileFormat());
    }

}
