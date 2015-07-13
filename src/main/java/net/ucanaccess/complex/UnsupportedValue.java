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
