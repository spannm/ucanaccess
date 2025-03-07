package net.ucanaccess.test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * A base test using an existing database file derived from test class name.
 */
public abstract class UcanaccessBaseFileTest extends UcanaccessBaseTest {

    @Override
    protected final String getAccessPath() {
        String prefix = getClass().getSimpleName();
        prefix = prefix.substring(0, prefix.lastIndexOf("Test"));
        prefix = Character.toLowerCase(prefix.charAt(0)) + prefix.substring(1);
        for (String ext : List.of(".accdb", ".mdb")) {
            File f = new File(getTestDbDir(), prefix + ext);
            if (f.exists()) {
                return f.getAbsolutePath();
            }
            String resource = getTestDbDir() + prefix + ext;
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resource)) {
                if (is != null) {
                    return resource;
                }
            } catch (IOException _ex) {
                continue;
            }
        }
        fail("Test " + getShortTestMethodName() + " requires a database file");
        return null;
    }

    @Override
    protected String getFileExtension() {
        return Optional.ofNullable(getAccessPath()).map(p -> p.substring(p.lastIndexOf('.'))).orElse(null);
    }

}
