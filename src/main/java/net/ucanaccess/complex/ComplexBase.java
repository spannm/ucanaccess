package net.ucanaccess.complex;

import io.github.spannm.jackcess.complex.ComplexDataType;
import io.github.spannm.jackcess.complex.ComplexValue;
import io.github.spannm.jackcess.complex.ComplexValueForeignKey;
import io.github.spannm.jackcess.impl.complex.ComplexColumnInfoImpl;
import net.ucanaccess.exception.UcanaccessSQLException;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public abstract class ComplexBase implements Serializable {
    private static final long           serialVersionUID = 1L;
    public static final ComplexValue.Id CREATE_ID        = ComplexColumnInfoImpl.INVALID_ID;

    private final int                   id;
    private String                      tableName;
    private String                      columnName;

    protected ComplexBase(ComplexValue.Id id, String tableName, String columnName) {
        this.id = id.get();
        this.tableName = tableName;
        this.columnName = columnName;
    }

    protected ComplexBase(ComplexValue cv) {
        this(cv.getId(), cv.getComplexValueForeignKey().getColumn().getTable().getName(),
            cv.getComplexValueForeignKey().getColumn().getName());
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (columnName == null ? 0 : columnName.hashCode());
        result = prime * result + id;
        result = prime * result + (tableName == null ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ComplexBase other = (ComplexBase) obj;
        if (columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        } else if (!columnName.equals(other.columnName)) {
            return false;
        }
        return id == other.id
            && Objects.equals(tableName, other.tableName);
    }

    public static Object[] convert(ComplexValueForeignKey fk) throws IOException, UcanaccessSQLException {
        if (fk.getComplexType().equals(ComplexDataType.ATTACHMENT)) {
            List<io.github.spannm.jackcess.complex.Attachment> lst = fk.getAttachments();
            Attachment[] lat = new Attachment[lst.size()];
            for (int i = 0; i < lat.length; i++) {
                lat[i] = new Attachment(lst.get(i));
            }
            return lat;
        }
        if (fk.getComplexType().equals(ComplexDataType.MULTI_VALUE)) {
            List<io.github.spannm.jackcess.complex.SingleValue> lst = fk.getMultiValues();
            SingleValue[] lat = new SingleValue[lst.size()];
            for (int i = 0; i < lat.length; i++) {
                lat[i] = new SingleValue(lst.get(i));
            }
            return lat;
        }

        if (fk.getComplexType().equals(ComplexDataType.VERSION_HISTORY)) {
            List<io.github.spannm.jackcess.complex.Version> lst = fk.getVersions();
            Version[] lat = new Version[lst.size()];
            for (int i = 0; i < lat.length; i++) {
                lat[i] = new Version(lst.get(i));
            }
            return lat;
        }
        if (fk.getComplexType().equals(ComplexDataType.UNSUPPORTED)) {
            List<io.github.spannm.jackcess.complex.UnsupportedValue> lst = fk.getUnsupportedValues();
            UnsupportedValue[] lat = new UnsupportedValue[lst.size()];
            for (int i = 0; i < lat.length; i++) {
                lat[i] = new UnsupportedValue(lst.get(i));
            }
            return lat;
        }
        throw new UcanaccessSQLException("Complex type not supported yet");
    }

    @Override
    public String toString() {
        return String.format("%s[id=%s, table=%s, column=%s]", getClass().getSimpleName(), id, tableName, columnName);
    }

    /**
     * Converts a local date-time (date/time without timezone) with arbitrary resolution (depends on Java version)
     * to millisecond-only resolution for compatibility with Jackcess.
     *
     * @param ldt local datetime with arbitrary resolution
     * @return local datetime with arbitrary resolution
     */
    static LocalDateTime handleJackcessLocalDateTimeResolution(LocalDateTime ldt) {
        if (ldt == null) {
            return ldt;
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(ldt.getNano());
        return ldt.withNano((int) TimeUnit.MILLISECONDS.toNanos(millis));
    }

}
