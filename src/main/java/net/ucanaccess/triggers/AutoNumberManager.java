package net.ucanaccess.triggers;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import net.ucanaccess.jdbc.DBReference;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class AutoNumberManager {
    // Consider replacing AtomicInteger with a custom wrapper around an 'int' if performance
    // becomes an issue. Never use an Integer here because Integer is an immutable object.
    private static final Map<Column, AtomicInteger> REGISTER = new HashMap<>();

    static {
        // Must call AutoNumberManager.clear() for proper thread synchronization.
        // Do not call register.clear() directly.
        DBReference.addOnReloadRefListener(AutoNumberManager::clear);
    }

    private AutoNumberManager() {
    }

    /** Clears all AutoNumber column seeds to 0. */
    static synchronized void clear() {
        REGISTER.clear();
    }

    /** Returns the next AutoNumber value, and increments the seed. */
    static synchronized int getNext(Column _col) {
        // Note: This code assumes *sequential* integer AutoNumber values.
        // (Access also supports *random* integer AutoNumber values, but they
        // are not very common.)
        ColumnImpl col = (ColumnImpl) _col;
        AtomicInteger next = REGISTER.get(col);
        if (next == null) {
            next = new AtomicInteger((Integer) col.getAutoNumberGenerator().getLast());
            REGISTER.put(col, next);
        }
        return next.incrementAndGet();
    }

    /** Sets the AutoNumber seed to {@code newVal}. */
    public static synchronized void reset(Column _col, int _newVal) {
        REGISTER.put(_col, new AtomicInteger(_newVal));
    }

    /** Bumps the AutoNumber seed to {@code newVal} if it is higher than the existing one. */
    public static synchronized void bump(Column _col, int _newVal) {
        ColumnImpl col = (ColumnImpl) _col;
        AtomicInteger next = REGISTER.get(col);
        if (next == null) {
            next = new AtomicInteger((Integer) col.getAutoNumberGenerator().getLast());
            REGISTER.put(col, next);
        }
        if (_newVal > next.get()) {
            next.set(_newVal);
        }
    }

}
