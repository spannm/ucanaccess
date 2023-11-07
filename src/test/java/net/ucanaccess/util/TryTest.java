package net.ucanaccess.util;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

class TryTest extends AbstractBaseTest {

    @Test
    void testOfNullThrowsNpe() {
        assertThrows(NullPointerException.class, () -> Try.catching((IThrowingSupplier<Void, Throwable>) null));
    }

    @Test
    void testThrowingSupplierDoesNotThrow() {
        Path tmpDir = Path.of(System.getProperty("java.io.tmpdir"));
        Try<FileTime, IOException> tryCatch = Try.catching(() -> Files.getLastModifiedTime(tmpDir));
        FileTime result = tryCatch.get();
        assertEquals("Try[val=" + result + ", ex=null]", tryCatch.toString());
        assertNotNull(result);
        assertNull(tryCatch.getException());
        assertFalse(tryCatch.hasThrown());
        assertEquals(result, tryCatch.orElse(FileTime.fromMillis(System.currentTimeMillis() + 1_000)));
        assertEquals(result, tryCatch.orElseApply(t -> fail("Should not have been called")));
        assertEquals(result, tryCatch.orElseGet(() -> fail("Should not have been called")));
        assertEquals(result, tryCatch.orThrow());
        assertEquals(result, tryCatch.orThrow(RuntimeException::new));

        Try<String, IOException> mappedtc = tryCatch.map(FileTime::toString);
        assertEquals(result.toString(), mappedtc.get());
        assertNull(mappedtc.getException());
        assertFalse(mappedtc.hasThrown());
    }

    @Test
    void testSupplierDoesNotThrow() {
        Try<String, Throwable> tryCatch = Try.catching(() -> "suprise");
        String result = tryCatch.get();
        assertEquals("Try[val=suprise, ex=null]", tryCatch.toString());
        assertNotNull(result);
        assertNull(tryCatch.getException());
        assertFalse(tryCatch.hasThrown());
        assertEquals(result, tryCatch.orElse("or_else"));
        assertEquals(result, tryCatch.orElseApply(t -> fail("Should not have been called")));
        assertEquals(result, tryCatch.orElseGet(() -> fail("Should not have been called")));
        assertEquals(result, tryCatch.orThrow());
        assertEquals(result, tryCatch.orThrow(RuntimeException::new));

        Try<String, Throwable> mappedtc = tryCatch.map(s -> "not suprised");
        assertEquals("not suprised", mappedtc.get());
        assertNull(mappedtc.getException());
        assertFalse(mappedtc.hasThrown());
    }

    @Test
    void testSupplierThrowsCheckedException() {
        String pathName = "hokus";
        Path notExistingPath = Path.of(pathName);
        Try<Long, IOException> tryCatch = Try.catching(() -> Files.size(notExistingPath));
        assertEquals("Try[val=null, ex=java.nio.file.NoSuchFileException: " + pathName + "]", tryCatch.toString());
        NoSuchFileException ex1 = assertThrows(NoSuchFileException.class, tryCatch::get);
        assertSame(ex1, tryCatch.getException());
        assertTrue(tryCatch.hasThrown());
        assertEquals(-1L, tryCatch.orElse(-1L));
        assertEquals(-1L, tryCatch.orElseApply(t -> -1L));
        assertEquals(-1L, tryCatch.orElseGet(() -> -1L));
        assertSame(ex1, assertThrows(IOException.class, tryCatch::orThrow));
        UncheckedIOException unioex = assertThrows(UncheckedIOException.class, () -> tryCatch.orThrow(UncheckedIOException::new));
        assertSame(ex1, unioex.getCause());

        Try<Boolean, Throwable> mappedtc = tryCatch.map(size -> {
            boolean greaterZero = size > 0; // will throw NPE but not even executed
            return greaterZero;
        });
        NoSuchFileException ex2 = assertThrows(NoSuchFileException.class, mappedtc::get);
        assertSame(ex2, mappedtc.getException()); // original exception unchanged
        assertSame(ex1, ex2);
        assertTrue(mappedtc.hasThrown());
    }

    @Test
    void testSupplierThrowsRuntimeException() {
        Try<Object, IllegalArgumentException> tryCatch = Try.catching(() -> {
            throw new IllegalArgumentException(getShortTestMethodName());
        });
        //
        assertEquals("Try[val=null, ex=java.lang.IllegalArgumentException: " + getShortTestMethodName() + "]", tryCatch.toString());
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, tryCatch::get);
        assertSame(exception, tryCatch.getException());
        assertTrue(tryCatch.hasThrown());
        assertEquals(-1L, tryCatch.orElse(-1L));
        assertEquals(-1L, tryCatch.orElseApply(t -> -1L));
        assertEquals(-1L, tryCatch.orElseGet(() -> -1L));
        assertSame(exception, assertThrows(IllegalArgumentException.class, tryCatch::orThrow));
        RuntimeException rtex = assertThrows(RuntimeException.class, () -> tryCatch.orThrow(RuntimeException::new));
        assertSame(exception, rtex.getCause());
    }

}
