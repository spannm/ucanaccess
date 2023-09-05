package net.ucanaccess.triggers;

import static org.mockito.Mockito.when;

import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.ColumnImpl.AutoNumberGenerator;
import net.ucanaccess.test.util.AbstractTestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AutoNumberManagerTest extends AbstractTestBase {

    @Mock
    private ColumnImpl          column;
    @Mock
    private AutoNumberGenerator autoNumberGenerator;

    @Before
    public void setUp() {
        when(column.getAutoNumberGenerator()).thenReturn(autoNumberGenerator);
        when(autoNumberGenerator.getLast()).thenReturn(1);
    }

    @Test
    public void testGetNext() {
        assertEquals(2, AutoNumberManager.getNext(column));
        assertEquals(3, AutoNumberManager.getNext(column));
        assertEquals(4, AutoNumberManager.getNext(column));
    }

    @Test
    public void testReset() {
        assertEquals(2, AutoNumberManager.getNext(column));
        assertEquals(3, AutoNumberManager.getNext(column));
        AutoNumberManager.reset(column, 0);
        assertEquals(1, AutoNumberManager.getNext(column));
    }

    @Test
    public void testBump() {
        assertEquals(2, AutoNumberManager.getNext(column));
        assertEquals(3, AutoNumberManager.getNext(column));
        AutoNumberManager.bump(column, 0);
        assertEquals(4, AutoNumberManager.getNext(column));
        AutoNumberManager.bump(column, 10);
        assertEquals(11, AutoNumberManager.getNext(column));
    }

    @Test
    public void testClear() {
        assertEquals(2, AutoNumberManager.getNext(column));
        assertEquals(3, AutoNumberManager.getNext(column));
        AutoNumberManager.clear();
        assertEquals(2, AutoNumberManager.getNext(column));
    }
}
