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
package net.ucanaccess.complex;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.complex.ComplexColumnInfoImpl;

public abstract  class ComplexBase implements Serializable {
	private static final long serialVersionUID = 1L;
	private int  id;
	private String tableName;
	private String columnName;
	 public final static ComplexValue.Id CREATE_ID=ComplexColumnInfoImpl.INVALID_ID;
	
	public ComplexBase(ComplexValue.Id id, String tableName, String columnName) {
		super();
		this.id = id.get();
		this.tableName = tableName;
		this.columnName = columnName;
	}
	public ComplexBase(ComplexValue cv) {
		this(cv.getId(),
				cv.getComplexValueForeignKey().getColumn().getTable().getName(),
				cv.getComplexValueForeignKey().getColumn().getName()
				);
	}
	

	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result + id;
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ComplexBase other = (ComplexBase) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (id != other.id)
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}
	
	public final static Object[] convert(ComplexValueForeignKey fk)
			throws IOException, UcanaccessSQLException {
		if (fk.getComplexType().equals(ComplexDataType.ATTACHMENT)) {
			List<com.healthmarketscience.jackcess.complex.Attachment> lst = fk
					.getAttachments();
			Attachment[] lat = new Attachment[lst.size()];
			for (int i = 0; i < lat.length; i++) {
				lat[i] = new Attachment(lst.get(i));
			}
			return lat;
		}
		if (fk.getComplexType().equals(ComplexDataType.MULTI_VALUE)) {
			List<com.healthmarketscience.jackcess.complex.SingleValue> lst = fk
					.getMultiValues();
			SingleValue[] lat = new SingleValue[lst.size()];
			for (int i = 0; i < lat.length; i++) {
				lat[i] = new SingleValue(lst.get(i));
			}
			return lat;
		}
		
		if (fk.getComplexType().equals(ComplexDataType.VERSION_HISTORY)) {
			List<com.healthmarketscience.jackcess.complex.Version> lst = fk
					.getVersions();
			Version[] lat = new Version[lst.size()];
			for (int i = 0; i < lat.length; i++) {
				lat[i] = new Version(lst.get(i));
			}
			return lat;
		}
		if (fk.getComplexType().equals(ComplexDataType.UNSUPPORTED)) {
			List<com.healthmarketscience.jackcess.complex.UnsupportedValue> lst = fk
					.getUnsupportedValues();
			UnsupportedValue[] lat = new UnsupportedValue[lst.size()];
			for (int i = 0; i < lat.length; i++) {
				lat[i] = new UnsupportedValue(lst.get(i));
			}
			return lat;
		}
		throw new UcanaccessSQLException(ExceptionMessages.COMPLEX_TYPE_UNSUPPORTED);
	}
	
}
