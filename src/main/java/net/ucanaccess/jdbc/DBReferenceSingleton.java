package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.Database.FileFormat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DBReferenceSingleton {
    private static DBReferenceSingleton    singleton;
    private final Map<String, DBReference> dbRegistry = Collections.synchronizedMap(new HashMap<>());

    private DBReferenceSingleton() {
    }

    public static synchronized DBReferenceSingleton getInstance() {
        if (singleton == null) {
            singleton = new DBReferenceSingleton();
        }
        return singleton;
    }

    public DBReference getReference(File ref) {
        return dbRegistry.get(ref.getAbsolutePath());
    }

    public boolean loaded(File fl) {
        return dbRegistry.containsKey(fl.getAbsolutePath());
    }

    public DBReference loadReference(File fl, FileFormat ff, IJackcessOpenerInterface jko, String pwd) throws IOException {
        return new DBReference(fl, ff, jko, pwd);
    }

    public DBReference put(String _path, DBReference _dbr) {
        return dbRegistry.put(_path, _dbr);
    }

    public DBReference remove(String _path) {
        synchronized (UcanaccessDriver.class) {
            return dbRegistry.remove(_path);
        }
    }

}
