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
package net.ucanaccess.converters;

import java.util.ResourceBundle;

public class RegionalSettings {
	
	
	private static  final ResourceBundle dateBundle=ResourceBundle.getBundle("net.ucanaccess.util.format.dateFormat");
	

	
	public static  String getAM() {
		return dateBundle.getString("AM");
	}
	
	public static  String getPM() {
		return dateBundle.getString("PM");
	}
	
	public static  String getRS() {
		return dateBundle.getString("RS");
	}
	
	public static  String getLongDatePattern() {
		return dateBundle.getString("longDate");
	}
	public  static   String getMediumDatePattern() {
		return dateBundle.getString("mediumDate");
	}
	public  static  String getShortDatePattern() {
		return dateBundle.getString("shortDate");
	}
	public  static  String getLongTimePattern() {
	 return dateBundle.getString("longTime");
	}
	public  static  String getMediumTimePattern() {
		 return  dateBundle.getString("mediumTime");
	}
	public  static String getShortTimePattern() {
		return dateBundle.getString("shortTime");
	}
	
	
	public  static String getGeneralPattern() {
		return dateBundle.getString("generalDate");
	}
	
	

}
