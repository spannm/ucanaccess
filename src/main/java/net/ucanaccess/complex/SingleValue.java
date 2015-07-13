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
	
	public static SingleValue[] multipleValue(String...rv){
		SingleValue[] sv=new SingleValue[rv.length];
		for(int j=0;j<rv.length;j++)
			sv[j]=new SingleValue(rv[j]);
		return sv;
	}
	
	
}
