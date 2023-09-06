package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.List;

public abstract class AccessVersionAllTest extends UcanaccessTestBase {

    public AccessVersionAllTest(AccessVersion _accessVersion) {
        super(_accessVersion.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<AccessVersion> getAllAccessVersions() {
        return List.of(
            AccessVersion.V2000,
            AccessVersion.V2003,
            AccessVersion.V2007,
            AccessVersion.V2010,
            AccessVersion.V2016);
    }

}
