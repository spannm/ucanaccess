package net.ucanaccess.complex;

import io.github.spannm.jackcess.complex.ComplexValue;

import java.time.LocalDateTime;

public class Version extends ComplexBase {

    private static final long serialVersionUID = 1L;
    private String            value;
    private LocalDateTime     modifiedDate;

    public Version(io.github.spannm.jackcess.complex.Version cv) {
        super(cv);
        value = cv.getValue();
        modifiedDate = cv.getModifiedLocalDate();
    }

    public Version(ComplexValue.Id id, String tableName, String columnName, String value, LocalDateTime modifiedDate) {
        super(id, tableName, columnName);
        this.value = value;
        this.modifiedDate = handleJackcessLocalDateTimeResolution(modifiedDate);
    }

    public Version(String value, LocalDateTime modifiedDate) {
        this(CREATE_ID, null, null, value, modifiedDate);
    }

    public LocalDateTime getModifiedDate() {
        return modifiedDate;
    }

    public void setModifiedDate(LocalDateTime modifiedDate) {
        this.modifiedDate = modifiedDate;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Version other = (Version) obj;
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
