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

import java.util.Map;

import com.healthmarketscience.jackcess.complex.ComplexValue;



public class UnsupportedValue extends ComplexBase{
	
	private static final long serialVersionUID = 1L;
	;
	private Map<String, Object> values;

	public UnsupportedValue(com.healthmarketscience.jackcess.complex.UnsupportedValue cv) {
		super(cv);
		this.values=cv.getValues();
	}

	public UnsupportedValue(ComplexValue.Id id, String tableName, String columnName, Map<String, Object> values) {
		super(id, tableName, columnName);
		this.values = values;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
//		if (!super.equals(obj))
//			return false;
		if (getClass() != obj.getClass())
			return false;
		UnsupportedValue other = (UnsupportedValue) obj;
		if (values == null) {
			if (other.values != null)
				return false;
		} else if (!values.equals(other.values))
			return false;
		return true;
	}

	public Map<String, Object> getValues() {
		return values;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		return result;
	}

	public void setValues(Map<String, Object> values) {
		this.values = values;
	}

	

	
	
	
}
