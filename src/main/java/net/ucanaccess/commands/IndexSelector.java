package net.ucanaccess.commands;

import io.github.spannm.jackcess.Cursor;
import io.github.spannm.jackcess.CursorBuilder;
import io.github.spannm.jackcess.Index;
import io.github.spannm.jackcess.Table;
import io.github.spannm.jackcess.complex.ComplexValueForeignKey;
import io.github.spannm.jackcess.util.SimpleColumnMatcher;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.util.Try;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public final class IndexSelector {

    private final Table table;
    private Index       bestIndex;
    private boolean     primaryCursor;

    public IndexSelector(Table table) {
        this.table = table;
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
            cursor = cb.withIndex(idx).toCursor();
        }
        cursor.setColumnMatcher(new ColumnMatcher());
        return cursor;
    }

    public boolean isPrimaryCursor() {
        return primaryCursor;
    }

    private static final class ColumnMatcher extends SimpleColumnMatcher {
        @Override
        public boolean matches(Table table, String columnName, Object currVal, Object dbVal) {

            if (currVal == null && dbVal == null) {
                return true;
            } else if (currVal == null || dbVal == null) {
                return false;
            } else if (currVal instanceof Date && dbVal instanceof Date) {
                return ((Date) currVal).compareTo((Date) dbVal) == 0;
            } else if (currVal instanceof BigDecimal && dbVal instanceof BigDecimal) {
                return ((BigDecimal) currVal).compareTo((BigDecimal) dbVal) == 0;
            } else if (dbVal instanceof BigDecimal && currVal instanceof Number) {
                return ((BigDecimal) dbVal).compareTo(new BigDecimal(currVal.toString())) == 0;
            } else if (currVal instanceof BigDecimal && dbVal instanceof Number) {
                return ((BigDecimal) currVal).compareTo(new BigDecimal(dbVal.toString())) == 0;
            } else if (currVal instanceof Integer && dbVal instanceof Short) {
                return (Integer) currVal == ((Short) dbVal).intValue();
            } else if (dbVal instanceof Integer && currVal instanceof Short) {
                return (Integer) dbVal == ((Short) currVal).intValue();
            } else if (currVal instanceof Integer && dbVal instanceof Byte) {
                return (Integer) currVal == SQLConverter.asUnsigned((Byte) dbVal);
            } else if (dbVal instanceof Integer && currVal instanceof Byte) {
                return (Integer) dbVal == SQLConverter.asUnsigned((Byte) currVal);
            }

            if (dbVal instanceof Float && currVal instanceof Double
                    || dbVal instanceof Double && currVal instanceof Float) {
                if (new BigDecimal(dbVal.toString()).compareTo(new BigDecimal(currVal.toString())) == 0) {
                    return true;
                }
                if (dbVal instanceof Float && currVal instanceof Double) {
                    return ((Float) dbVal).doubleValue() == (Double) currVal;
                }
            }
            if (currVal instanceof byte[] && dbVal instanceof byte[]) {
                return true;
            }

            if (currVal instanceof ComplexBase[] && dbVal instanceof ComplexValueForeignKey) {
                return Try.catching(() -> Arrays.equals((ComplexBase[]) currVal, ComplexBase.convert((ComplexValueForeignKey) dbVal)))
                    .orThrow(UcanaccessRuntimeException::new);
            }

            return super.matches(table, columnName, currVal, dbVal);
        }
    }
}
