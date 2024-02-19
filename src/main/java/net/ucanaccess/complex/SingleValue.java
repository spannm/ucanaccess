package net.ucanaccess.complex;

import io.github.spannm.jackcess.complex.ComplexValue;

public class SingleValue extends ComplexBase {

    private static final long serialVersionUID = 1L;
    private Object            value;

    public SingleValue(io.github.spannm.jackcess.complex.SingleValue cv) {
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

    public Object getValue() {
        return value;
    }

    public void setValue(Object _value) {
        value = _value;
    }

    @Override
    public boolean equals(Object _obj) {
        if (this == _obj) {
            return true;
        } else if (_obj == null || getClass() != _obj.getClass()) {
            return false;
        }
        SingleValue other = (SingleValue) _obj;
        if (value == null) {
            return other.value == null;
        } else {
            return value.equals(other.value);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (value == null ? 0 : value.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s[value=%s]", getClass().getSimpleName(), value);
    }

    public static SingleValue[] multipleValue(String... rv) {
        SingleValue[] sv = new SingleValue[rv.length];
        for (int j = 0; j < rv.length; j++) {
            sv[j] = new SingleValue(rv[j]);
        }
        return sv;
    }

}
