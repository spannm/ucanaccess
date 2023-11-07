package net.ucanaccess.util;

/**
 * An operation that accepts one argument, produces no result and may throw an exception.
 * <p>
 * This is a functional interface whose functional method is {@link #accept(Object) accept(I)}.
 *
 * @param <I> the type of the input to the consumer
 * @param <T> the type of Throwable thrown
 *
 * @author Markus Spann
 * @since v5.1.0
 */
@FunctionalInterface
public interface IThrowingConsumer<I, T extends Throwable> {

    /**
     * Applies this operation to the given argument throwing an exception of type {@code T}.
     *
     * @param _input the consumer argument
     */
    void accept(I _input) throws T;

}
