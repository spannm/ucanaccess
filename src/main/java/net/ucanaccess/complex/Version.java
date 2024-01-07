package net.ucanaccess.complex;

import com.healthmarketscience.jackcess.complex.ComplexValue;

import java.time.LocalDateTime;

public class Version extends ComplexBase {

    private static final long serialVersionUID = 1L;
    private String            value;
    private LocalDateTime     modifiedDate;

    public Version(com.healthmarketscience.jackcess.complex.Version cv) {
        super(cv);
        value = cv.getValue();
        modifiedDate = cv.getModifiedLocalDate();
    }

    public Version(ComplexValue.Id id, String tableName, String columnName, String _value, LocalDateTime _modifiedDate) {
        super(id, tableName, columnName);
        value = _value;
        modifiedDate = handleJackcessLocalDateTimeResolution(_modifiedDate);
    }

    public Version(String _value, LocalDateTime _modifiedDate) {
        this(CREATE_ID, null, null, _value, _modifiedDate);
    }

    public LocalDateTime getModifiedDate() {
        return modifiedDate;
    }

    public void setModifiedDate(LocalDateTime _modifiedDate) {
        modifiedDate = _modifiedDate;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String _value) {
        value = _value;
    }

    @Override
    public boolean equals(Object _obj) {
        if (this == _obj) {
            return true;
        } else if (_obj == null || getClass() != _obj.getClass()) {
            return false;
        }
        Version other = (Version) _obj;
        if (modifiedDate == null) {
            if (other.modifiedDate != null) {
                return false;
            }
        } else if (!modifiedDate.equals(other.modifiedDate)) {
            return false;
        }
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
        result = prime * result + (modifiedDate == null ? 0 : modifiedDate.hashCode());
        result = prime * result + (value == null ? 0 : value.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s[value=%s, modifiedDate=%s]", getClass().getSimpleName(), value, modifiedDate);
    }

}
