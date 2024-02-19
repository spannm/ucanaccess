package net.ucanaccess.triggers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.spannm.jackcess.impl.ColumnImpl;
import io.github.spannm.jackcess.impl.ColumnImpl.AutoNumberGenerator;
import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AutoNumberManagerTest extends AbstractBaseTest {

    private ColumnImpl          column;
    private AutoNumberGenerator autoNumberGenerator;

    @BeforeEach
    void setUp() {
        column = mock(ColumnImpl.class);
        autoNumberGenerator = mock(AutoNumberGenerator.class);
        when(column.getAutoNumberGenerator()).thenReturn(autoNumberGenerator);
        when(autoNumberGenerator.getLast()).thenReturn(1);
    }

    @Test
    void testGetNext() {
        assertEquals(2, AutoNumberManager.getNext(column));
        assertEquals(3, AutoNumberManager.getNext(column));
        assertEquals(4, AutoNumberManager.getNext(column));
    }

    @Test
    void testReset() {
        assertEquals(2, AutoNumberManager.getNext(column));
        assertEquals(3, AutoNumberManager.getNext(column));
        AutoNumberManager.reset(column, 0);
        assertEquals(1, AutoNumberManager.getNext(column));
    }

    @Test
    void testBump() {
        assertEquals(2, AutoNumberManager.getNext(column));
        assertEquals(3, AutoNumberManager.getNext(column));
        AutoNumberManager.bump(column, 0);
        assertEquals(4, AutoNumberManager.getNext(column));
        AutoNumberManager.bump(column, 10);
        assertEquals(11, AutoNumberManager.getNext(column));
    }

    @Test
    void testClear() {
        assertEquals(2, AutoNumberManager.getNext(column));
        assertEquals(3, AutoNumberManager.getNext(column));
        AutoNumberManager.clear();
        assertEquals(2, AutoNumberManager.getNext(column));
    }
}
