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
package net.ucanaccess.jdbc;


import net.ucanaccess.util.Logger;

public class FeatureNotSupportedException extends java.sql.SQLFeatureNotSupportedException {

	public enum NotSupportedMessage{
		NOT_SUPPORTED,
		NOT_SUPPORTED_YET
		
	}
	private static final long serialVersionUID = -6457220326288384415L;

	public FeatureNotSupportedException() {
		this(NotSupportedMessage.NOT_SUPPORTED);
		
	}
	public FeatureNotSupportedException(NotSupportedMessage message) {
		super(Logger.getMessage(message.name()));
		
	}


}
