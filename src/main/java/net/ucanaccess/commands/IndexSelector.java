/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package net.ucanaccess.commands;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.converters.SQLConverter;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.util.SimpleColumnMatcher;

public class IndexSelector {
    private class ColumnMatcher extends SimpleColumnMatcher {
        @Override
        public boolean matches(Table table, String columnName, Object currVal, Object dbVal) {

            if (currVal == null && dbVal == null) {
                return true;
            }
            if (currVal == null || dbVal == null) {
                return false;
            }
            if (currVal instanceof Date && dbVal instanceof Date) {
                return ((Date) currVal).compareTo((Date) dbVal) == 0;
            }
            if (currVal instanceof BigDecimal && dbVal instanceof BigDecimal) {
                return ((BigDecimal) currVal).compareTo((BigDecimal) dbVal) == 0;
            }
            if (dbVal instanceof BigDecimal && currVal instanceof Number) {
                return ((BigDecimal) dbVal).compareTo(new BigDecimal(currVal.toString())) == 0;
            }

            if (currVal instanceof BigDecimal && dbVal instanceof Number) {
                return ((BigDecimal) currVal).compareTo(new BigDecimal(dbVal.toString())) == 0;
            }

            if (currVal instanceof Integer && dbVal instanceof Short) {
                return ((Integer) currVal).intValue() == ((Short) dbVal).intValue();
            }
            if (dbVal instanceof Integer && currVal instanceof Short) {
                return ((Integer) dbVal).intValue() == ((Short) currVal).intValue();
            }
            if (currVal instanceof Integer && dbVal instanceof Byte) {
                return ((Integer) currVal).intValue() == SQLConverter.asUnsigned((Byte) dbVal);
            }
            if (dbVal instanceof Integer && currVal instanceof Byte) {
                return ((Integer) dbVal).intValue() == SQLConverter.asUnsigned((Byte) currVal);

            }

            if ((dbVal instanceof Float && currVal instanceof Double)
                    || (dbVal instanceof Double && currVal instanceof Float)) {
                if (new BigDecimal(dbVal.toString()).compareTo(new BigDecimal(currVal.toString())) == 0) {
                    return true;
                }
                if (dbVal instanceof Float && currVal instanceof Double) {
                    return ((Float) dbVal).doubleValue() == ((Double) currVal);
                }
            }
            if (currVal instanceof byte[] && dbVal instanceof byte[]) {
                byte[] val1 = (byte[]) currVal;
                byte[] val2 = (byte[]) dbVal;
                if (val1.length != val2.length) {
                    return false;
                }
                for (int y = 0; y < val1.length; y++) {
                    if (val1[y] != val2[y]) {
                        return false;
                    }
                }

                return true;
            }

            if (currVal instanceof ComplexBase[] && dbVal instanceof ComplexValueForeignKey) {
                try {
                    boolean eq =
                            Arrays.equals((ComplexBase[]) currVal, ComplexBase.convert((ComplexValueForeignKey) dbVal));
                    return eq;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            return super.matches(table, columnName, currVal, dbVal);
        }
    }

    private Index   bestIndex;
    private boolean primaryCursor;
    private Table   table;

    public IndexSelector(Table table) {
        super();
        this.table = table;
    }

    public Index getBestIndex() {
        if (this.bestIndex == null) {
            List<? extends Index> li = table.getIndexes();
            for (Index idx : li) {
                if (idx.isPrimaryKey()) {
                    this.bestIndex = idx;
                    this.primaryCursor = true;
                    break;
                }
            }
            if (this.bestIndex == null) {
                for (Index idx : li) {
                    if (idx.isUnique()) {
                        this.bestIndex = idx;
                        break;
                    }
                }
            }
            if (this.bestIndex == null && li.size() == 1) {
                this.bestIndex = li.get(0);
            }
        }
        return this.bestIndex;
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
}
