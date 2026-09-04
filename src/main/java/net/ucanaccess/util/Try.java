package net.ucanaccess.util;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Fluent-style try-catch handling.
 *
 * <p>{@code Try} objects are constructed using the {@link #catching(IThrowingRunnable)} or {@link #catching(IThrowingSupplier)} factories
 * from code blocks which may or may not throw checked or unchecked (run-time) exceptions.<br>
 * The exception is dealt with inside the {@code Try} instance.
 * The class provides methods to access the result, access the exception,
 * use an alternative result if an exception occurred, transform the result to another result amongst other methods.<br>
 * {@code Try} instances are immutable.
 *
 * <p>Usage example:
 * <pre>
 *     // In this sample we query the last modification timestamp of a path p.
 *     // The API throws IOException (if an I/O error occurs).
 *
 *     Path p = Path.of("a_path");
 *
 *     // Code using a conventional try/catch construct looks like this:
 *     String str1 = null;
 *     try {
 *         str1 = Files.getLastModifiedTime(p).toString();
 *     } catch (IOException ex) {
 *         str1 = "unknown";
 *     }
 *     System.out.println("1: File time is: " + str1);
 *
 *     // The same using Try:
 *     Try&lt;FileTime, IOException&gt; tc1 = Try.catching(() -&gt; Files.getLastModifiedTime(p));
 *
 *     // Perform a transformation of the internal value. Here FileTime is mapped to its string representation.
 *     Try&lt;String, Throwable&gt; tc2 = tc1.map(FileTime::toString);
 *
 *     String str2 = tc2.orElse("unknown");
 *     System.out.println("2: File time is: " + str2);
 *
 *     // The same on a single line:
 *     System.out.println("3: File time is: " + Try.catching(() -&gt; Files.getLastModifiedTime(p).toString()).orElse("unknown"));
 *
 *     // Similarly the class supports try-with-resources scenarios such as:
 *     List&lt;File&gt; files = Try.withResources(() -&gt; Files.walk(p), w -&gt; {
 *         return w.filter(Files::isRegularFile)
 *                 .map(Path::toFile)
 *                 .collect(Collectors.toList());
 *     }).orIgnore();
 * </pre>
 *
 * @param <V> the type of the optional value from the code block executed within @code try}
 * @param <EC> the type of exception, if any, thrown by the executed code block.
 *
 * @author Markus Spann
 * @since v5.1.0
 */
public final class Try<V, EC extends Throwable> {

    /** The immutable value or {@code null} if an exception occurred during retrieval. */
    private final V  val;
    /** The immutable exception or {@code null} if value retrieval succeeded without exception. */
    private final EC t;

    /**
     * Creates a new instance from a throwing supplier.
     *
     * @param catchable a throwing supplier
     */
    private Try(IThrowingSupplier<V, EC> catchable) {
        V locVal = null;
        EC locEx = null;
        try {
            locVal = catchable.get();
        } catch (Throwable ex) {
            @SuppressWarnings("unchecked")
            EC castEx = (EC) ex;
            locEx = castEx;
        }
        val = locVal;
        t = locEx;
    }

    /**
     * Creates a new instance from a value and an exception.
     *
     * @param val value, may be null
     * @param ex exception, may be null
     */
    private Try(V val, EC ex) {
        this.val = val;
        t = ex;
    }

    /**
     * Creates a new instance from a throwing {@link Supplier}.
     *
     * @param <R> the type of value supplied by the supplier
     * @param <EC> the type of exception, if any, thrown by the supplier
     * @param catchable a throwing supplier
     * @return new instance
     */
    public static <R, EC extends Throwable> Try<R, EC> catching(IThrowingSupplier<R, EC> catchable) {
        return new Try<>(Objects.requireNonNull(catchable, "Supplier required"));
    }

    /**
     * Creates a new instance from a throwing {@link Runnable}.
     *
     * @param <EC> the type of exception, if any, thrown by the runnable
     * @param catchable a throwing runnable
     * @return new instance
     */
    public static <EC extends Throwable> Try<Void, EC> catching(IThrowingRunnable<EC> catchable) {
        Objects.requireNonNull(catchable, "Runnable required");
        return new Try<>(() -> { // convert to throwing supplier returning null
            catchable.run();
            return null;
        });
    }

    /**
     * Try-with-resources support for a single resource.<br>
     * The resource is created and returned by {@code resourceSupplier} and processed in {@code catchable}.<br>
     * If an exception occurs during resource creation, this execution is thrown and thus processing ends.
     *
     * @param <R> the type of resource (must implement {@link AutoCloseable})
     * @param <ES> the type of exception, if any, thrown by the resource supplier
     * @param <V> the type of value returned by the function
     * @param <EC> the type of exception, if any, thrown by the function
     *
     * @param resourceSupplier the resource supplier
     * @param catchable the function executed on the resource
     * @return new instance
     */
    public static <R extends AutoCloseable, ES extends Throwable, V, EC extends Throwable> Try<V, EC> withResources(
        IThrowingSupplier<R, ES> resourceSupplier, IThrowingFunction<R, V, EC> catchable) {

        Objects.requireNonNull(resourceSupplier, "Resource supplier required");
        Objects.requireNonNull(catchable, "Resource consumer required");
        try (R r = resourceSupplier.get()) {
            V locVal = null;
            EC locEx = null;
            try {
                locVal = catchable.apply(r);
            } catch (Throwable ex) {
                // this block catches exceptions thrown by the 'catchable' function
                // tf it's an InterruptedException, restore the interrupt status
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                @SuppressWarnings("unchecked")
                EC castEx = (EC) ex;
                locEx = castEx;
            }
            return new Try<>(locVal, locEx);
        } catch (Throwable ex) {
            // this block catches exceptions from two sources:
            // 1. The 'resourceSupplier.get()' call (exceptions of type ES)
            // 2. The implicit 'r.close()' call of the AutoCloseable resource

            // If an InterruptedException occurs here (e.g., during resource closing),
            // it's crucial to restore the thread's interrupted status
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            doThrow(ex);
            return null; // technically unreachable due to doThrow(ex)
        }
    }

    /**
     * Try-with-resources support for a single resource with a {@link Void} (no) result.
     */
    public static <R extends AutoCloseable, ES extends Throwable, EC extends Throwable> Try<Void, EC> withResources(
        IThrowingSupplier<R, ES> resourceSupplier, IThrowingConsumer<R, EC> catchable) {

        Objects.requireNonNull(resourceSupplier, "Resource supplier required");
        Objects.requireNonNull(catchable, "Resource consumer required");
        return withResources(resourceSupplier, i -> { // convert to throwing function returning null
            catchable.accept(i);
            return null;
        });
    }

    /**
     * Performs a transformation on the internal value, if the source object does not have an exception.
     *
     * @param <V1> the type result produced by the mapper
     * @param <E1> the type of exception thrown by the mapper
     * @param mapper mapping function
     * @return new instance with transformed value
     */
    public <V1, E1 extends Throwable> Try<V1, E1> map(Function<? super V, ? extends V1> mapper) {
        Objects.requireNonNull(mapper, "Mapper required");
        if (t != null) {
            @SuppressWarnings("unchecked")
            E1 e1 = (E1) t;
            return new Try<>((V1) null, e1);
        }
        return catching((IThrowingSupplier<V1, E1>) () -> mapper.apply(val));
    }

    /**
     * Gets the value or re-throws the exception if one has occurred.
     *
     * @return the successful value
     */
    public V get() {
        if (hasThrown()) {
            doThrow(t);
        }
        return val;
    }

    /**
     * Gets the exception or {@code null} if none has occurred.
     * @return exception
     */
    public EC getException() {
        return t;
    }

    /**
     * Indicates whether an exception has occurred.
     *
     * @return {@code true} if an exception has occurred, {@code false} otherwise.
     */
    boolean hasThrown() {
        return null != t;
    }

    /**
     * Returns the successful value or if an exception has occurred returns {@code other}.
     *
     * @param other the value to be returned, if an exception has occurred. May be {@code null}.
     * @return the successful value, otherwise the {@code other} value
     */
    public V orElse(V other) {
        return hasThrown() ? other : val;
    }

    /**
     * Executes {@code consumer} with the exception as parameter, if an exception has occurred.<br>
     * If an exception occurs during execution of the throwing consumer, that exception is thrown.
     * @param consumer consumer to execute in case of exception. The exception object is passed to the consumer.
     */
    public void orElse(IThrowingConsumer<EC, Throwable> consumer) {
        if (hasThrown()) {
            catching(() -> consumer.accept(t)).orThrow();
        }
    }

    /**
     * If an exception has occurred, executes {@code function} with the exception as parameter
     * to retrieve an alternative value, otherwise returns the value.
     * @param function function to execute in case of exception. The exception object is passed to the function.
     * @return the successful value or value returned by the function
     */
    public V orElseApply(IThrowingFunction<EC, V, Throwable> function) {
        if (hasThrown()) {
            return catching(() -> function.apply(t)).orThrow();
        }
        return val;
    }

    /**
     * If an exception has occurred, executes {@code supplier} to get an alternative value, otherwise returns the value.
     * @param supplier supplier that supplies an alternative value
     * @return the successful value or value returned by the supplier
     */
    public V orElseGet(IThrowingSupplier<V, Throwable> supplier) {
        if (hasThrown()) {
            return catching(supplier::get).orThrow();
        }
        return val;
    }

    /**
     * Returns the value on success or {@code null} ignoring any prior exception.
     *
     * @return the successful value or null
     */
    public V orIgnore() {
        return hasThrown() ? null : val;
    }

    /**
     * Returns the successful value or throws the exception that occurred earlier.
     * @return the successful value
     */
    public V orThrow() {
        if (hasThrown()) {
            doThrow(t);
        }
        return val;
    }

    /**
     * Returns the successful value or throws the exception returned by {@code function}.<br>
     * The function is applied if this object has previously thrown and must provide an exception.
     *
     * @param <T2> the type of exception returned by {@code function}
     * @param function non-null function to provide an exception. The previous exception object is passed to the function.
     * @return the successful value
     * @throws T2 the exception returned by {@code function}
     */
    public <T2 extends Throwable> V orThrow(Function<EC, T2> function) throws T2 {
        Objects.requireNonNull(function, "Function required");
        if (hasThrown()) {
            Throwable t2 = function.apply(t);
            if (t2 == null) {
                doThrow(new IllegalStateException("Function must provide a throwable"));
            } else {
                doThrow(t2);
            }
        }
        return val;
    }

    @Override
    public String toString() {
        return String.format("%s[val=%s, ex=%s]", getClass().getSimpleName(), val, t);
    }

    /**
     * Performs a vacuous cast on a Throwable to evade the Java checked exception mechanism.
     * @param <T> the type of Throwable
     * @param t throwable
     * @return run-time exception
     * @throws T throwable
     */
    @SuppressWarnings("unchecked")
    private static <T extends Throwable> RuntimeException doThrow(Throwable t) throws T {
        throw (T) t;
    }

}
