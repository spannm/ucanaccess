package net.ucanaccess.test.util;

import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

public abstract class AccessVersionDefaultTest extends UcanaccessTestBase {

    public AccessVersionDefaultTest(AccessVersion _fileFormat) {
        super(_fileFormat.getFileFormat());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> getDefaultAccessVersion() {
        List<Object[]> list = new ArrayList<Object[]>();
        list.add(new Object[] {DEFAULT_ACCESS_VERSION});
        return list;
    }

}
