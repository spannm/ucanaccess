package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database.FileFormat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Central registry for database references in the UCanaccess driver.
 * <p>
 * Manages the lifecycle and access to {@link DBReference} instances using a singleton pattern.
 * It ensures that multiple requests for the same database file can be coordinated.
 */
public final class DBReferenceSingleton {
    private static final DBReferenceSingleton SINGLETON  = new DBReferenceSingleton();
    private final Map<String, DBReference>    dbRegistry = Collections.synchronizedMap(new LinkedHashMap<>());

    private DBReferenceSingleton() {
    }

    /**
     * Returns the singleton instance of the registry.
     * @return the global DBReferenceSingleton instance
     */
    public static synchronized DBReferenceSingleton getInstance() {
        return SINGLETON;
    }

    /**
     * Retrieves a cached reference for a given file.
     * @param ref the database file
     * @return the associated DBReference or null if not loaded
     */
    public DBReference getReference(File ref) {
        return dbRegistry.get(ref.getAbsolutePath());
    }

    /**
     * Checks if a database file is already registered.
     * @param fl the database file to check
     * @return true if the file is present in the registry
     */
    public boolean loaded(File fl) {
        return dbRegistry.containsKey(fl.getAbsolutePath());
    }

    /**
     * Creates a new DBReference instance.
     * <p>
     * Note: this method does not automatically add the reference to the registry.
     * @throws IOException if the database access fails
     */
    public DBReference loadReference(File fl, FileFormat ff, IJackcessOpenerInterface jko, String pwd, Charset charset) throws IOException {
        return new DBReference(fl, ff, jko, pwd, charset);
    }

    /**
     * Adds a database reference to the registry.
     * @param _path absolute path of the database file
     * @param _dbr the reference instance
     * @return the previous reference associated with the path, or null
     */
    public DBReference put(String _path, DBReference _dbr) {
        return dbRegistry.put(_path, _dbr);
    }

    /**
     * Removes a reference from the registry.
     * <p>
     * This operation is synchronized on UcanaccessDriver to ensure driver-wide consistency.
     * @param _path absolute path to be removed
     * @return the removed DBReference or null
     */
    public DBReference remove(String _path) {
        // synchronized on the driver class to prevent race conditions during deregistration
        synchronized (UcanaccessDriver.class) {
            return dbRegistry.remove(_path);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[dbRegistry=" + dbRegistry.keySet() + ']';
    }

}
