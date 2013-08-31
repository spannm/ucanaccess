/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

*/
package net.ucanaccess.commands;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import net.ucanaccess.complex.ComplexBase;



import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.SimpleColumnMatcher;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;


public class IndexSelector {
	private class ColumnMatcher extends SimpleColumnMatcher {
		@Override
		public boolean matches(Table table, String columnName, Object currVal,
				 Object dbVal) {
			if (currVal == null && dbVal == null)
				return true;
			if (currVal == null || dbVal == null)
				return false;
			if (currVal instanceof Date && dbVal instanceof Date) {
				return ((Date) currVal).compareTo((Date) dbVal) == 0;
			}
			if (currVal instanceof BigDecimal && dbVal instanceof BigDecimal) {
				return ((BigDecimal) currVal).compareTo((BigDecimal) dbVal) == 0;
			}
			if (dbVal instanceof BigDecimal && currVal instanceof Number) {
				return ((BigDecimal) dbVal).compareTo(new BigDecimal( currVal.toString()))==0;
			}
			
			if (currVal instanceof BigDecimal && dbVal instanceof Number) {
				return ((BigDecimal) currVal).compareTo(new BigDecimal( dbVal.toString()))==0;
			}
			
			if (currVal instanceof Integer && dbVal instanceof Short) {
				return ((Integer) currVal).intValue() == ((Short) dbVal)
						.intValue();
			}
			if (dbVal instanceof Integer && currVal instanceof Short) {
				return ((Integer) dbVal).intValue() == ((Short) currVal)
						.intValue();
			}
			if (currVal instanceof Integer && dbVal instanceof Byte) {
				return ((Integer) currVal).intValue() == ((Byte) dbVal)
						.intValue();
			}
			if (dbVal instanceof Integer && currVal instanceof Byte) {
				return ((Integer) dbVal).intValue() == ((Byte) currVal)
						.intValue();
			}
			
			if ((dbVal instanceof Float && currVal instanceof Double)||(dbVal instanceof Double && currVal instanceof Float)) {
				return new BigDecimal(dbVal.toString()).compareTo(new BigDecimal( currVal.toString()))==0;
			}
			if (currVal instanceof byte[]
					&& dbVal instanceof byte[]) {
				byte[] val1=(byte[])currVal;
				byte[] val2=(byte[])dbVal;
				if(val1.length!=val2.length)return false;
				for (int y = 0; y <val1.length; y++) {
					if(val1[y]!=val2[y])return false;
				}

				return true;
			}
			
			
			if(currVal instanceof ComplexBase[]&& dbVal instanceof ComplexValueForeignKey){
				try {
					boolean eq=Arrays.equals((ComplexBase[])currVal, ComplexBase.convert((ComplexValueForeignKey)dbVal));
					return eq;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		
			return super.matches(table, columnName, currVal, dbVal);
		}
	}
	private Index bestIndex;
	private boolean primaryCursor;
	private Table table;
	
	public IndexSelector(Table table) {
		super();
		this.table = table;
	}
	
	public Index getBestIndex() {
		if (this.bestIndex == null) {
			List<Index> li = table.getIndexes();
			for (Index idx : li) {
				if (idx.isPrimaryKey()) {
					this.bestIndex = idx;
					this.primaryCursor = true;
					break;
				}
			}
			if (this.bestIndex == null)
				for (Index idx : li) {
					if (idx.isUnique()) {
						this.bestIndex = idx;
						break;
					}
				}
			if (this.bestIndex == null && li.size() == 1)
				this.bestIndex = li.get(0);
		}
		return this.bestIndex;
	}
	
	public Cursor getCursor() throws IOException {
		Index idx = getBestIndex();
		Cursor cursor;
		if (idx == null)
			cursor = Cursor.createCursor(table);
		else
			cursor = Cursor.createIndexCursor(table, idx);
		cursor.setColumnMatcher(new ColumnMatcher());
		return cursor;
	}
	
	public boolean isPrimaryCursor() {
		return primaryCursor;
	}
}
