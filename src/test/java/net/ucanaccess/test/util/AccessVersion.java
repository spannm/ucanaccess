package net.ucanaccess.test.util;

import com.healthmarketscience.jackcess.Database.FileFormat;

import java.util.List;

/**
 * Enum of MS Access Versions to encapsulate Jackcess FileFormat. To be used for test purposes only.
 */
public enum AccessVersion {

    V2000(FileFormat.V2000),
    V2003(FileFormat.V2003),
    V2007(FileFormat.V2007),
    V2010(FileFormat.V2010),
    V2016(FileFormat.V2016);

    static final AccessVersion DEFAULT_ACCESS_VERSION = AccessVersion.V2003;

    private final FileFormat fileFormat;

    AccessVersion(FileFormat _fileFormat) {
        fileFormat = _fileFormat;
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }

    public static List<AccessVersion> getDefaultAccessVersion() {
        return List.of(DEFAULT_ACCESS_VERSION);
    }

}
