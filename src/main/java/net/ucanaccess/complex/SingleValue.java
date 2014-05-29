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

import com.healthmarketscience.jackcess.complex.ComplexValue;



public class SingleValue extends ComplexBase{
	
	private static final long serialVersionUID = 1L;
	private Object value;

	public SingleValue(com.healthmarketscience.jackcess.complex.SingleValue cv) {
		super(cv);
		this.value=cv.get();
	}
	
	public SingleValue(ComplexValue.Id id, String tableName, String columnName, String value) {
		super(id, tableName, columnName);
		this.value = value;
	}

	public SingleValue( String value) {
		this(CREATE_ID, null, null,value);
		
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		SingleValue other = (SingleValue) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "SingleValue [value=" + value +"]";
	}
	
	
}
