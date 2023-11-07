package net.ucanaccess.util;

/**
 * A runnable which might throw an exception of type {@code T}.
 * <p>
 * This is a <a href="package-summary.html">functional interface</a> whose functional method is {@link #run()}.
 *
 * @param <T> the type of Throwable thrown
 *
 * @author Markus Spann
 * @since v5.1.0
 */
@FunctionalInterface
public interface IThrowingRunnable<T extends Throwable> {
    void run() throws T;
}
