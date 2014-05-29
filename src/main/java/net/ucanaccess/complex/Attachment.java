/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

*/
package net.ucanaccess.complex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import com.healthmarketscience.jackcess.complex.ComplexValue;

public class Attachment extends ComplexBase {
	private static final long serialVersionUID = 1L;
	private String url;
	private String name;
	private String type;
	private byte[] data;
	private Date timeStamp;
	private Integer flags;

	public Attachment(com.healthmarketscience.jackcess.complex.Attachment atc) throws IOException {
		super(atc);
		this.url = atc.getFileUrl();
		this.name = atc.getFileName();
		this.type = atc.getFileType();
		this.data = atc.getFileData();
		this.timeStamp = atc.getFileTimeStamp();
		this.flags = atc.getFileFlags();
	}
	public Attachment(ComplexValue.Id id, String tableName, String columnName, String url,
			String name, String type, byte[] data, Date timeStamp, Integer flags) {
		super(id, tableName, columnName);
		this.url = url;
		this.name = name;
		this.type = type;
		this.data = data;
		this.timeStamp = timeStamp;
		this.flags = flags;
	}

	public Attachment( String url,
			String name, String type, byte[] data, Date timeStamp, Integer flags) {
		this(CREATE_ID, null, null, url,
				name,  type,  data, timeStamp, flags);
		
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
//		if (!super.equals(obj))
//			return false;
		if (getClass() != obj.getClass())
			return false;
		Attachment other = (Attachment) obj;
		if (!Arrays.equals(data, other.data))
			return false;
		if (flags == null) {
			if (other.flags != null)
				return false;
		} else if (!flags.equals(other.flags))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (timeStamp == null) {
			if (other.timeStamp != null)
				return false;
		} else if (!timeStamp.equals(other.timeStamp))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
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

	public Date getTimeStamp() {
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
		result = prime * result + ((flags == null) ? 0 : flags.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((timeStamp == null) ? 0 : timeStamp.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((url == null) ? 0 : url.hashCode());
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

	public void setTimeStamp(Date timeStamp) {
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
		return "Attachment [url=" + url + ", name=" + name + ", type=" + type
				+ ", data=" + Arrays.toString(data) + ", timeStamp="
				+ timeStamp + ", flags=" + flags + "]";
	}

	
}
