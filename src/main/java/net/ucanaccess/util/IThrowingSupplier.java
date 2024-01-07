package net.ucanaccess.util;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A supplier of a result of type {@code R} which might throw an exception of type {@code T}.
 * <p>
 * This is a <a href="package-summary.html">functional interface</a> whose functional method is {@link #get()}.
 *
 * @param <R> the type of the result supplied
 * @param <T> the type of Throwable thrown
 *
 * @author Markus Spann
 * @since v5.1.0
 */
@FunctionalInterface
public interface IThrowingSupplier<R, T extends Throwable> {

    /**
     * Gets a result.
     *
     * @return a result
     */
    R get() throws T;

    /**
     * Creates a {@link IThrowingSupplier} from a {@link Supplier}.
     * @param <R> the type of the result supplied
     * @param <T> the type of Throwable thrown
     * @param _supplier a conventional supplier
     * @return a throwing supplier
     */
    static <R, T extends Throwable> IThrowingSupplier<R, T> of(Supplier<R> _supplier) {
        return () -> _supplier.get();
    }

    /**
     * Returns the supplied value, unless {@link #get()} threw an exception,
     * in which case the value produced by the supplying function is returned.
     *
     * @param _function non-null function executed in case of exception. The exception occurred in the supplier is passed to this function
     * @return a value
     */
    default R or(Function<Throwable, R> _function) {
        Objects.requireNonNull(_function, "Function required");
        try {
            return get();
        } catch (Throwable _ex) {
            return _function.apply(_ex);
        }
    }

    /**
     * Returns a {@link Supplier} for this throwing supplier.
     * @return supplier
     */
    default Supplier<R> toSupplier() {
        return new Supplier<R>() {
            @Override
            public R get() {
                try {
                    return IThrowingSupplier.this.get();
                } catch (Throwable _ex) {
                    throw doThrow(_ex);
                }
            }

            @SuppressWarnings("unchecked")
            <T1 extends Throwable> RuntimeException doThrow(Throwable _t) throws T1 {
                throw (T1) _t; // perform vacuous cast
            }

        };
    }

}
