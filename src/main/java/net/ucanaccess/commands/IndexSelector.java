package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.util.SimpleColumnMatcher;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.util.Try;
import net.ucanaccess.util.UcanaccessRuntimeException;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public final class IndexSelector {

    private final Table table;
    private Index       bestIndex;
    private boolean     primaryCursor;

    public IndexSelector(Table _table) {
        table = _table;
    }

    public Index getBestIndex() {
        if (bestIndex == null) {
            List<? extends Index> li = table.getIndexes();
            for (Index idx : li) {
                if (idx.isPrimaryKey()) {
                    bestIndex = idx;
                    primaryCursor = true;
                    break;
                }
            }
            if (bestIndex == null) {
                for (Index idx : li) {
                    if (idx.isUnique()) {
                        bestIndex = idx;
                        break;
                    }
                }
            }
            if (bestIndex == null && li.size() == 1) {
                bestIndex = li.get(0);
            }
        }
        return bestIndex;
    }

    public Cursor getCursor() throws IOException {
        Index idx = getBestIndex();
        Cursor cursor;
        CursorBuilder cb = table.newCursor();
        if (idx == null) {
            cursor = cb.toCursor();
        } else {
            cursor = cb.setIndex(idx).toCursor();
        }
        cursor.setColumnMatcher(new ColumnMatcher());
        return cursor;
    }

    public boolean isPrimaryCursor() {
        return primaryCursor;
    }

    private static final class ColumnMatcher extends SimpleColumnMatcher {
        @Override
        public boolean matches(Table _table, String _columnName, Object _currVal, Object _dbVal) {

            if (_currVal == null && _dbVal == null) {
                return true;
            } else if (_currVal == null || _dbVal == null) {
                return false;
            } else if (_currVal instanceof Date && _dbVal instanceof Date) {
                return ((Date) _currVal).compareTo((Date) _dbVal) == 0;
            } else if (_currVal instanceof BigDecimal && _dbVal instanceof BigDecimal) {
                return ((BigDecimal) _currVal).compareTo((BigDecimal) _dbVal) == 0;
            } else if (_dbVal instanceof BigDecimal && _currVal instanceof Number) {
                return ((BigDecimal) _dbVal).compareTo(new BigDecimal(_currVal.toString())) == 0;
            } else if (_currVal instanceof BigDecimal && _dbVal instanceof Number) {
                return ((BigDecimal) _currVal).compareTo(new BigDecimal(_dbVal.toString())) == 0;
            } else if (_currVal instanceof Integer && _dbVal instanceof Short) {
                return (Integer) _currVal == ((Short) _dbVal).intValue();
            } else if (_dbVal instanceof Integer && _currVal instanceof Short) {
                return (Integer) _dbVal == ((Short) _currVal).intValue();
            } else if (_currVal instanceof Integer && _dbVal instanceof Byte) {
                return (Integer) _currVal == SQLConverter.asUnsigned((Byte) _dbVal);
            } else if (_dbVal instanceof Integer && _currVal instanceof Byte) {
                return (Integer) _dbVal == SQLConverter.asUnsigned((Byte) _currVal);
            }

            if (_dbVal instanceof Float && _currVal instanceof Double
                    || _dbVal instanceof Double && _currVal instanceof Float) {
                if (new BigDecimal(_dbVal.toString()).compareTo(new BigDecimal(_currVal.toString())) == 0) {
                    return true;
                }
                if (_dbVal instanceof Float && _currVal instanceof Double) {
                    return ((Float) _dbVal).doubleValue() == (Double) _currVal;
                }
            }
            if (_currVal instanceof byte[] && _dbVal instanceof byte[]) {
                return true;
            }

            if (_currVal instanceof ComplexBase[] && _dbVal instanceof ComplexValueForeignKey) {
                return Try.catching(() -> Arrays.equals((ComplexBase[]) _currVal, ComplexBase.convert((ComplexValueForeignKey) _dbVal)))
                    .orThrow(UcanaccessRuntimeException::new);
            }

            return super.matches(_table, _columnName, _currVal, _dbVal);
        }
    }
}
