package net.ucanaccess.complex;

import io.github.spannm.jackcess.complex.ComplexValue;

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

    public Attachment(io.github.spannm.jackcess.complex.Attachment atc) throws IOException {
        super(atc);
        url = atc.getFileUrl();
        name = atc.getFileName();
        type = atc.getFileType();
        data = atc.getFileData();
        timeStamp = atc.getFileLocalTimeStamp();
        flags = atc.getFileFlags();
    }

    public Attachment(ComplexValue.Id id, String tableName, String columnName, String url, String name, String type,
            byte[] data, LocalDateTime timeStamp, Integer flags) {
        super(id, tableName, columnName);
        this.url = url;
        this.name = name;
        this.type = type;
        this.data = data;
        this.timeStamp = handleJackcessLocalDateTimeResolution(timeStamp);
        this.flags = flags;
    }

    public Attachment(String url, String name, String type, byte[] data, LocalDateTime timeStamp, Integer flags) {
        this(CREATE_ID, null, null, url, name, type, data, timeStamp, flags);

    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
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
        }
        return url.equals(other.url);
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

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setFlags(Integer flags) {
        this.flags = flags;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTimeStamp(LocalDateTime timeStamp) {
        this.timeStamp = timeStamp;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return String.format("%s[url=%s, name=%s, type=%s, data=%s, timeStamp=%s, flags=%s]",
            getClass().getSimpleName(),  url, name, type, Arrays.toString(data), timeStamp, flags);
    }

}
