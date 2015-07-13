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
package net.ucanaccess.converters;

import java.util.Locale;
import java.util.ResourceBundle;

public class RegionalSettings {
	
	
	private   final ResourceBundle dateBundle;
	
	
	public RegionalSettings(){
		this.dateBundle=ResourceBundle.getBundle("net.ucanaccess.util.format.dateFormat");
	}
	
	public RegionalSettings(Locale l){
		this.dateBundle=ResourceBundle.getBundle("net.ucanaccess.util.format.dateFormat",l);
	}


	
	public   String getAM() {
		return dateBundle.getString("AM");
	}
	
	public   String getPM() {
		return dateBundle.getString("PM");
	}
	
	public   String getRS() {
		return dateBundle.getString("RS");
	}
	
	public   String getLongDatePattern() {
		return dateBundle.getString("longDate");
	}
	public     String getMediumDatePattern() {
		return dateBundle.getString("mediumDate");
	}
	public    String getShortDatePattern() {
		return dateBundle.getString("shortDate");
	}
	public    String getLongTimePattern() {
	 return dateBundle.getString("longTime");
	}
	public    String getMediumTimePattern() {
		 return  dateBundle.getString("mediumTime");
	}
	public  String getShortTimePattern() {
		return dateBundle.getString("shortTime");
	}
		
	public String getGeneralPattern() {
		return dateBundle.getString("generalDate");
	}
	
	

}
