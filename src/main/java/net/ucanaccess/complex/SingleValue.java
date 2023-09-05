package net.ucanaccess.complex;

import com.healthmarketscience.jackcess.complex.ComplexValue;

public class SingleValue extends ComplexBase {

    private static final long serialVersionUID = 1L;
    private Object            value;

    public SingleValue(com.healthmarketscience.jackcess.complex.SingleValue cv) {
        super(cv);
        value = cv.get();
    }

    public SingleValue(ComplexValue.Id id, String tableName, String columnName, String _value) {
        super(id, tableName, columnName);
        value = _value;
    }

    public SingleValue(String _value) {
        this(CREATE_ID, null, null, _value);

    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SingleValue other = (SingleValue) obj;
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (value == null ? 0 : value.hashCode());
        return result;
    }

    public void setValue(Object _value) {
        value = _value;
    }

    @Override
    public String toString() {
        return "SingleValue [value=" + value + "]";
    }

    public static SingleValue[] multipleValue(String... rv) {
        SingleValue[] sv = new SingleValue[rv.length];
        for (int j = 0; j < rv.length; j++) {
            sv[j] = new SingleValue(rv[j]);
        }
        return sv;
    }

}
