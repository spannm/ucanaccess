package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.List;

public abstract class AccessVersion2010Test extends UcanaccessTestBase {

    public AccessVersion2010Test(AccessVersion _accessVersion) {
        super(_accessVersion.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<AccessVersion> getAccessVersion2010() {
        return List.of(AccessVersion.V2010);
    }

}
