package net.ucanaccess.test.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.runners.Parameterized;

public abstract class AccessVersionDefaultTest extends UcanaccessTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> getDefaultAccessVersion() {
        List<Object[]> list = new ArrayList<Object[]>();
        list.add(new Object[] { DEFAULT_ACCESS_VERSION });
        return list;
    }

    public AccessVersionDefaultTest(AccessVersion _fileFormat) {
        super(_fileFormat.getFileFormat());
    }
}
