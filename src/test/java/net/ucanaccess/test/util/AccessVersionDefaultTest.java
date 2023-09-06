package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.List;

public abstract class AccessVersionDefaultTest extends UcanaccessTestBase {

    public AccessVersionDefaultTest(AccessVersion _accessVersion) {
        super(_accessVersion.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<AccessVersion> getDefaultAccessVersion() {
        return List.of(DEFAULT_ACCESS_VERSION);
    }

}
