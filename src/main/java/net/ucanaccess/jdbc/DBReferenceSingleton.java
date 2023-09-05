package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.Database.FileFormat;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DBReferenceSingleton {
    private static DBReferenceSingleton singletonObject;
    private Map<String, DBReference>    dbRegistry = Collections.synchronizedMap(new HashMap<>());

    private DBReferenceSingleton() {
    }

    public static DBReferenceSingleton getInstance() {
        if (singletonObject == null) {
            singletonObject = new DBReferenceSingleton();
        }
        return singletonObject;
    }

    public DBReference getReference(File ref) {
        return dbRegistry.get(ref.getAbsolutePath());
    }

    public boolean loaded(File fl) throws IOException, SQLException {
        return dbRegistry.containsKey(fl.getAbsolutePath());
    }

    public DBReference loadReference(File fl, FileFormat ff, JackcessOpenerInterface jko, String pwd)
            throws IOException, SQLException {
        DBReference ref = new DBReference(fl, ff, jko, pwd);
        return ref;
    }

    public DBReference put(String path, DBReference dbr) {
        return dbRegistry.put(path, dbr);
    }

    public DBReference remove(String path) throws IOException, SQLException {
        synchronized (UcanaccessDriver.class) {
            return dbRegistry.remove(path);
        }
    }

}
