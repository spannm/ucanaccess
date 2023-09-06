package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.List;

public abstract class AccessVersion2016Test extends UcanaccessTestBase {

    public AccessVersion2016Test(AccessVersion _accessVersion) {
        super(_accessVersion.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<AccessVersion> getAccessVersion2016() {
        return List.of(AccessVersion.V2016);
    }

}
