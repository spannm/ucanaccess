package net.ucanaccess.triggers;

import io.github.spannm.jackcess.Column;
import io.github.spannm.jackcess.impl.ColumnImpl;
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
    static synchronized int getNext(Column col) {
        // Note: This code assumes *sequential* integer AutoNumber values.
        // (Access also supports *random* integer AutoNumber values, but they
        // are not very common.)
        return seed(col).incrementAndGet();
    }

    /** Sets the AutoNumber seed to {@code newVal}. */
    public static synchronized void reset(Column col, int newVal) {
        REGISTER.put(col, new AtomicInteger(newVal));
    }

    /** Bumps the AutoNumber seed to {@code newVal} if it is higher than the existing one. */
    public static synchronized void bump(Column col, int newVal) {
        AtomicInteger next = seed(col);
        if (newVal > next.get()) {
            next.set(newVal);
        }
    }

    private static AtomicInteger seed(Column col) {
        return REGISTER.computeIfAbsent(col,
            c -> new AtomicInteger((Integer) ((ColumnImpl) c).getAutoNumberGenerator().getLast()));
    }

}
