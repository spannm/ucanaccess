package net.ucanaccess.complex;

import java.util.Map;

import com.healthmarketscience.jackcess.complex.ComplexValue;

public class UnsupportedValue extends ComplexBase {

    private static final long   serialVersionUID = 1L;;
    private Map<String, Object> values;

    public UnsupportedValue(com.healthmarketscience.jackcess.complex.UnsupportedValue cv) {
        super(cv);
        this.values = cv.getValues();
    }

    public UnsupportedValue(ComplexValue.Id id, String tableName, String columnName, Map<String, Object> _values) {
        super(id, tableName, columnName);
        this.values = _values;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        // if (!super.equals(obj))
        // return false;
        if (getClass() != obj.getClass()) {
            return false;
        }
        UnsupportedValue other = (UnsupportedValue) obj;
        if (values == null) {
            if (other.values != null) {
                return false;
            }
        } else if (!values.equals(other.values)) {
            return false;
        }
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

    public void setValues(Map<String, Object> _values) {
        this.values = _values;
    }

}
