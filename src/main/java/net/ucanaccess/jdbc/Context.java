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
package net.ucanaccess.jdbc;


public class Context {
	private UcanaccessConnection currentConnection;
	private String currentExecId;
	
	
	public Context(UcanaccessConnection currentConnection) {
		super();
		this.currentConnection = currentConnection;
	}
	public UcanaccessConnection getCurrentConnection() {
		return currentConnection;
	}
	public String getCurrentExecId() {
		return currentExecId;
	}
	public void setCurrentConnection(UcanaccessConnection currentConnection) {
		this.currentConnection = currentConnection;
	}
	public void setCurrentExecId(String currentExecId) {
		this.currentExecId = currentExecId;
	}
	
	
}
