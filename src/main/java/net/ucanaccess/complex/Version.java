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

import java.util.Date;

import com.healthmarketscience.jackcess.complex.ComplexValue;



public class Version extends ComplexBase{
	
	private static final long serialVersionUID = 1L;
	private String value;
	 private Date modifiedDate;
	 public Version(com.healthmarketscience.jackcess.complex.Version cv) {
		super(cv);
		this.value=cv.getValue();
		this.modifiedDate=cv.getModifiedDate();
	}
	 
	  public Version(ComplexValue.Id id, String tableName, String columnName, String value,
				Date modifiedDate) {
			super(id, tableName, columnName);
			this.value = value;
			this.modifiedDate = modifiedDate;
		}
	  
	public Version( String value,
				Date modifiedDate) {
		this(CREATE_ID, null, null, value, modifiedDate);
		}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		Version other = (Version) obj;
		if (modifiedDate == null) {
			if (other.modifiedDate != null)
				return false;
		} else if (!modifiedDate.equals(other.modifiedDate))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	public Date getModifiedDate() {
		return modifiedDate;
	}
	
	public String getValue() {
		return value;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((modifiedDate == null) ? 0 : modifiedDate.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}
	public void setModifiedDate(Date modifiedDate) {
		this.modifiedDate = modifiedDate;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return "Version [value=" + value + ", modifiedDate=" + modifiedDate
				+ "]";
	}
	
}
