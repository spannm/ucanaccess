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
