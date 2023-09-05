package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

public abstract class AccessVersion2007Test extends UcanaccessTestBase {

    public AccessVersion2007Test(AccessVersion _fileFormat) {
        super(_fileFormat.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> getAccessVersion2007() {
        List<Object[]> list = new ArrayList<Object[]>();
        list.add(new Object[] {AccessVersion.V2007});
        return list;
    }

}
