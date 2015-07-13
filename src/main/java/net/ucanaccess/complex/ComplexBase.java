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
