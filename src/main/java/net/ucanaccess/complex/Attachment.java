package net.ucanaccess.complex;

import com.healthmarketscience.jackcess.complex.ComplexValue;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;

public class Attachment extends ComplexBase {
    private static final long serialVersionUID = 1L;
    private String            url;
    private String            name;
    private String            type;
    private byte[]            data;
    private LocalDateTime     timeStamp;
    private Integer           flags;

    public Attachment(com.healthmarketscience.jackcess.complex.Attachment atc) throws IOException {
        super(atc);
        url = atc.getFileUrl();
        name = atc.getFileName();
        type = atc.getFileType();
        data = atc.getFileData();
        timeStamp = atc.getFileLocalTimeStamp();
        flags = atc.getFileFlags();
    }

    public Attachment(ComplexValue.Id id, String tableName, String columnName, String _url, String _name, String _type,
            byte[] _data, LocalDateTime _timeStamp, Integer _flags) {
        super(id, tableName, columnName);
        url = _url;
        name = _name;
        type = _type;
        data = _data;
        timeStamp = _timeStamp;
        flags = _flags;
    }

    public Attachment(String _url, String _name, String _type, byte[] _data, LocalDateTime _timeStamp, Integer _flags) {
        this(CREATE_ID, null, null, _url, _name, _type, _data, _timeStamp, _flags);

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
        Attachment other = (Attachment) obj;
        if (!Arrays.equals(data, other.data)) {
            return false;
        }
        if (flags == null) {
            if (other.flags != null) {
                return false;
            }
        } else if (!flags.equals(other.flags)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (timeStamp == null) {
            if (other.timeStamp != null) {
                return false;
            }
        } else if (!timeStamp.equals(other.timeStamp)) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!type.equals(other.type)) {
            return false;
        }
        if (url == null) {
            return other.url == null;
        } else if (!url.equals(other.url)) {
            return false;
        }
        return true;
    }

    public byte[] getData() {
        return data;
    }

    public Integer getFlags() {
        return flags;
    }

    public String getName() {
        return name;
    }

    public LocalDateTime getTimeStamp() {
        return timeStamp;
    }

    public String getType() {
        return type;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Arrays.hashCode(data);
        result = prime * result + (flags == null ? 0 : flags.hashCode());
        result = prime * result + (name == null ? 0 : name.hashCode());
        result = prime * result + (timeStamp == null ? 0 : timeStamp.hashCode());
        result = prime * result + (type == null ? 0 : type.hashCode());
        result = prime * result + (url == null ? 0 : url.hashCode());
        return result;
    }

    public void setData(byte[] _data) {
        data = _data;
    }

    public void setFlags(Integer _flags) {
        flags = _flags;
    }

    public void setName(String _name) {
        name = _name;
    }

    public void setTimeStamp(LocalDateTime _timeStamp) {
        timeStamp = _timeStamp;
    }

    public void setType(String _type) {
        type = _type;
    }

    public void setUrl(String _url) {
        url = _url;
    }

    @Override
    public String toString() {
        return "Attachment [url=" + url + ", name=" + name + ", type=" + type + ", data=" + Arrays.toString(data)
                + ", timeStamp=" + timeStamp + ", flags=" + flags + "]";
    }

}
