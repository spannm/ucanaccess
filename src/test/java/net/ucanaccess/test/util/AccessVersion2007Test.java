package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.List;

public abstract class AccessVersion2007Test extends UcanaccessTestBase {

    public AccessVersion2007Test(AccessVersion _accessVersion) {
        super(_accessVersion.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<AccessVersion> getAccessVersion2007() {
        return List.of(AccessVersion.V2007);
    }

}
