package net.ucanaccess.util;

/**
 * A function that accepts one argument, produces a result and may throw an exception.
 * <p>
 * This is a functional interface whose functional method is {@link #apply(Object) apply(I)}.
 *
 * @param <I> the type of the input to the function
 * @param <R> the type of the result of the function
 * @param <T> the type of Throwable thrown
 *
 * @author Markus Spann
 * @since v5.1.0
 */
@FunctionalInterface
public interface IThrowingFunction<I, R, T extends Throwable> {
    /**
     * Applies this function to the given argument possibly throwing an exception of type {@code T}.
     *
     * @param _input the function argument
     * @return the function result
     */
    R apply(I _input) throws T;

}
