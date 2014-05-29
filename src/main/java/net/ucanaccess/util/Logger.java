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
package net.ucanaccess.util;


import java.io.PrintWriter;
import java.util.ResourceBundle;
import java.util.logging.Level;

public class Logger {
	public enum Messages{
		HSQLDB_DRIVER_NOT_FOUND,
		COMPLEX_TYPE_UNSUPPORTED,
		KEEP_MIRROR_AND_OTHERS,
		UNKNOWN_EXPRESSION,
		DEFAULT_VALUES_DELIMETERS
	}
	private static PrintWriter logPrintWriter;  
	private static ResourceBundle messageBundle=ResourceBundle.getBundle("net.ucanaccess.util.messages");
	
	public static void dump() {
		StackTraceElement[] ste = Thread.currentThread().getStackTrace();
		for (StackTraceElement el : ste) {
			logPrintWriter.println(el.toString());
			logPrintWriter.flush();
		}
	}
	
	public static void turnOffJackcessLog(){
		java.util.logging.Logger logger = java.util.logging.Logger
		.getLogger("com.healthmarketscience.jackcess");
         logger.setLevel(Level.OFF);
	}
	
	public static PrintWriter getLogPrintWriter() {
		return logPrintWriter;
	}
	
	public static String getMessage(String cod){
		return messageBundle.getString(cod);
	}
	
	public static void log(Object obj) {
		if (logPrintWriter != null){
			logPrintWriter.println(obj);
			logPrintWriter.flush();
		}
	}
	
	public static void  logMessage(Messages cod){
		log( messageBundle.getString(cod.name()));
	}
	
	public static String  getLogMessage(Messages cod){
		return messageBundle.getString(cod.name());
	}
	
	public static void  logWarning(String warning){
		System.err.println("WARNING:"+ warning);
	}
	public static void  logWarning(Messages cod){
		logWarning( messageBundle.getString(cod.name()));
	}
	
	public static void  logParametricWarning(Messages cod,String... par){
		logWarning(String.format(messageBundle.getString(cod.name()),(Object[])par));
	}	
	public static void setLogPrintWriter(PrintWriter logPrintWriter) {
		Logger.logPrintWriter = logPrintWriter;
	}
}
