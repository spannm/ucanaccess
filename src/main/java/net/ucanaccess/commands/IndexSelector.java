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
        public boolean matches(Table _table, String _columnName, Object _currVal, Object _dbVal) {

            if (_currVal == null && _dbVal == null) {
                return true;
            }
            if (_currVal == null || _dbVal == null) {
                return false;
            }
            if (_currVal instanceof Date && _dbVal instanceof Date) {
                return ((Date) _currVal).compareTo((Date) _dbVal) == 0;
            }
            if (_currVal instanceof BigDecimal && _dbVal instanceof BigDecimal) {
                return ((BigDecimal) _currVal).compareTo((BigDecimal) _dbVal) == 0;
            }
            if (_dbVal instanceof BigDecimal && _currVal instanceof Number) {
                return ((BigDecimal) _dbVal).compareTo(new BigDecimal(_currVal.toString())) == 0;
            }

            if (_currVal instanceof BigDecimal && _dbVal instanceof Number) {
                return ((BigDecimal) _currVal).compareTo(new BigDecimal(_dbVal.toString())) == 0;
            }

            if (_currVal instanceof Integer && _dbVal instanceof Short) {
                return ((Integer) _currVal).intValue() == ((Short) _dbVal).intValue();
            }
            if (_dbVal instanceof Integer && _currVal instanceof Short) {
                return ((Integer) _dbVal).intValue() == ((Short) _currVal).intValue();
            }
            if (_currVal instanceof Integer && _dbVal instanceof Byte) {
                return ((Integer) _currVal).intValue() == SQLConverter.asUnsigned((Byte) _dbVal);
            }
            if (_dbVal instanceof Integer && _currVal instanceof Byte) {
                return ((Integer) _dbVal).intValue() == SQLConverter.asUnsigned((Byte) _currVal);

            }

            if ((_dbVal instanceof Float && _currVal instanceof Double)
                    || (_dbVal instanceof Double && _currVal instanceof Float)) {
                if (new BigDecimal(_dbVal.toString()).compareTo(new BigDecimal(_currVal.toString())) == 0) {
                    return true;
                }
                if (_dbVal instanceof Float && _currVal instanceof Double) {
                    return ((Float) _dbVal).doubleValue() == ((Double) _currVal);
                }
            }
            if (_currVal instanceof byte[] && _dbVal instanceof byte[]) {
                byte[] val1 = (byte[]) _currVal;
                byte[] val2 = (byte[]) _dbVal;
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

            if (_currVal instanceof ComplexBase[] && _dbVal instanceof ComplexValueForeignKey) {
                try {
                    boolean eq =
                            Arrays.equals((ComplexBase[]) _currVal, ComplexBase.convert((ComplexValueForeignKey) _dbVal));
                    return eq;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            return super.matches(_table, _columnName, _currVal, _dbVal);
        }
    }

    private Index   bestIndex;
    private boolean primaryCursor;
    private Table   table;

    public IndexSelector(Table _table) {
        this.table = _table;
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
