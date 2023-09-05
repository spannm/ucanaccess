package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

public abstract class AccessVersion2010Test extends UcanaccessTestBase {

    public AccessVersion2010Test(AccessVersion _fileFormat) {
        super(_fileFormat.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> getAccessVersion2010() {
        List<Object[]> list = new ArrayList<Object[]>();
        list.add(new Object[] {AccessVersion.V2010});
        return list;
    }

}
