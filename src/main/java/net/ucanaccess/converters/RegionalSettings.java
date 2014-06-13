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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;

public class RegionalSettings {
	private static ArrayList<String> supportedLocale=new ArrayList<String>();
	private static final HashMap<String,String>longDateHM=new HashMap<String,String>();
	private static final HashMap<String,String>mediumDateHM=new HashMap<String,String>();
	private static final HashMap<String,String>shortDateHM=new HashMap<String,String>();
	private static final HashMap<String,String>longTimeHM=new HashMap<String,String>();
	private static final HashMap<String,String>mediumTimeHM=new HashMap<String,String>();
	private static final HashMap<String,String>shortTimeHM=new HashMap<String,String>();
	private static final HashMap<String,String>generalDateHM=new HashMap<String,String>();
	private static final HashMap<String,String>lanDefaultHM=new HashMap<String,String>();
	
	static {
//		longDateHM.put("af_ZA","dd MMMM yyyy");
//		mediumDateHM.put("af_ZA","dd-MMM-yy");
//		shortDateHM.put("af_ZA","yyyy/MM/dd");
//		longTimeHM.put("af_ZA","hh:mm:ss a");
//		mediumTimeHM.put("af_ZA","hh:mm a");
//		shortTimeHM.put("af_ZA","HH:mm");
//		generalDateHM.put("af_ZA","yyyy/MM/dd hh:mm:ss a");
//		
//		longDateHM.put("sq_AL","yyyy-MM-dd");
//		mediumDateHM.put("sq_AL","dd-MMM-yy");
//		shortDateHM.put("sq_AL","yyyy-MM-dd");
//		longTimeHM.put("sq_AL","h:mm:ss.a");
//		mediumTimeHM.put("sq_AL","hh:mm a");
//		shortTimeHM.put("sq_AL","HH:mm");
//		generalDateHM.put("sq_AL","yyyy-MM-dd h:mm:ss.a");
//		
//		longDateHM.put("ar_DZ","dd MMMM, yyyy");
//		mediumDateHM.put("ar_DZ","dd-MMM-yyyy");
//		shortDateHM.put("ar_DZ","dd-MM-yyyy");
//		longTimeHM.put("ar_DZ","H:mm:ss");
//		mediumTimeHM.put("ar_DZ","hh:mm a");
//		shortTimeHM.put("ar_DZ","HH:mm");
//		generalDateHM.put("ar_DZ","dd-MM-yyyy H:mm:ss");
		
//		longDateHM.put("ar_SA","dd/MMMM/yyyy");
//		mediumDateHM.put("ar_SA","dd-MMM-''yyyy");
//		shortDateHM.put("ar_SA","dd/MM/yy");
//		longTimeHM.put("ar_SA","hh:mm:ss a");
//		mediumTimeHM.put("ar_SA","hh:mm a");
//		shortTimeHM.put("ar_SA","HH:mm");
//		generalDateHM.put("ar_SA","dd/MM/yy hh:mm:ss a");
//		
//		
//		longDateHM.put("hy_AM","d MMMM, yyyy");
//		mediumDateHM.put("hy_AM","d-MMM-yy");
//		shortDateHM.put("hy_AM","dd.MM.yyyy");
//		longTimeHM.put("hy_AM","H:mm:ss");
//		mediumTimeHM.put("hy_AM","hh:mm ");
//		shortTimeHM.put("hy_AM","HH:mm");
//		generalDateHM.put("hy_AM","dd.MM.yyyy H:mm:ss");
		
		supportedLocale.add("en_US");
		lanDefaultHM.put("en","en_US");
		longDateHM.put("en_US","EEEE, MMMM dd, yyyy");
		mediumDateHM.put("en_US","dd-MMM-yy");
		shortDateHM.put("en_US","M/d/yyyy");
		longTimeHM.put("en_US","h:mm:ss a");
		mediumTimeHM.put("en_US","hh:mm a");
		shortTimeHM.put("en_US","HH:mm");
		generalDateHM.put("en_US","M/d/yyyy h:mm:ss a");
		
		supportedLocale.add("zh_CN");
		lanDefaultHM.put("zh","zh_CN");
		longDateHM.put("zh_CN","yyyy MMM dd");
		mediumDateHM.put("zh_CN","yy-MM-dd");
		shortDateHM.put("zh_CN","yyyy/M/d");
		longTimeHM.put("zh_CN","H:mm:ss");
		mediumTimeHM.put("zh_CN","hh:mm a");
		shortTimeHM.put("zh_CN","HH:mm");
		generalDateHM.put("zh_CN","yyyy/M/d H:mm:ss");
		
		supportedLocale.add("zh_SG");
		longDateHM.put("zh_SG","yyyy MMM dd");
		mediumDateHM.put("zh_SG","dd-MMM-yy");
		shortDateHM.put("zh_SG","d/M/yyyy");
		longTimeHM.put("zh_SG","a h:mm:ss");
		mediumTimeHM.put("zh_SG","a hh:mm");
		shortTimeHM.put("zh_SG","HH:mm");
		generalDateHM.put("zh_SG","d/M/yyyy a h:mm:ss");
		
		
		supportedLocale.add("es_ES");
		lanDefaultHM.put("es","es_ES");
		longDateHM.put("es_ES","EEEE, dd' de 'MMMM' de 'yyyy");
		mediumDateHM.put("es_ES","dd-MMM-yy");
		shortDateHM.put("es_ES","dd/MM/yyyy");
		longTimeHM.put("es_ES","H:mm:ss");
		mediumTimeHM.put("es_ES","hh:mm ");
		shortTimeHM.put("es_ES","HH:mm");
		generalDateHM.put("es_ES","dd/MM/yyyy H:mm:ss");
		
		supportedLocale.add("de_DE");
		lanDefaultHM.put("de","de_DE");
		longDateHM.put("de_DE","EEEE, d. MMMM yyyy");
		mediumDateHM.put("de_DE","dd. MMM. yy");
		shortDateHM.put("de_DE","dd.MM.yyyy");
		longTimeHM.put("de_DE","HH:mm:ss");
		mediumTimeHM.put("de_DE","hh:mm ");
		shortTimeHM.put("de_DE","HH:mm");
		generalDateHM.put("de_DE","dd.MM.yyyy HH:mm:ss");
		
		supportedLocale.add("it_IT");
		lanDefaultHM.put("it","it_IT");
		longDateHM.put("it_IT","EEEE d MMMM yyyy");
		mediumDateHM.put("it_IT","dd-MMM-yy");
		shortDateHM.put("it_IT","dd/MM/yyyy");
		longTimeHM.put("it_IT","HH:mm:ss");
		mediumTimeHM.put("it_IT","hh:mm ");
		shortTimeHM.put("it_IT","HH:mm");
		generalDateHM.put("it_IT","dd/MM/yyyy HH:mm:ss");
		
		supportedLocale.add("fr_FR");
		lanDefaultHM.put("fr","fr_FR");
		longDateHM.put("fr_FR","EEEE d MMMM yyyy");
		mediumDateHM.put("fr_FR","dd-MMM-yy");
		shortDateHM.put("fr_FR","dd/MM/yyyy");
		longTimeHM.put("fr_FR","HH:mm:ss");
		mediumTimeHM.put("fr_FR","hh:mm ");
		shortTimeHM.put("fr_FR","HH:mm");
		generalDateHM.put("fr_FR","dd/MM/yyyy HH:mm:ss");
		
		supportedLocale.add("pt_BR");
		lanDefaultHM.put("pt","pt_BR");
		longDateHM.put("pt_BR","EEEE, d' de 'MMMM' de 'yyyy");
		mediumDateHM.put("pt_BR","dd/MMM/yy");
		shortDateHM.put("pt_BR","dd/MM/yyyy");
		longTimeHM.put("pt_BR","HH:mm:ss");
		mediumTimeHM.put("pt_BR","hh:mm ");
		shortTimeHM.put("pt_BR","HH:mm");
		generalDateHM.put("pt_BR","dd/MM/yyyy HH:mm:ss");
		
		supportedLocale.add("ru_RU");
		lanDefaultHM.put("ru","ru_RU");
		longDateHM.put("ru_RU","d MMMM yyyy 'г.'");
		mediumDateHM.put("ru_RU","dd-MMM-yy");
		shortDateHM.put("ru_RU","dd.MM.yyyy");
		longTimeHM.put("ru_RU","H:mm:ss");
		mediumTimeHM.put("ru_RU","hh:mm ");
		shortTimeHM.put("ru_RU","HH:mm");
		generalDateHM.put("ru_RU","dd.MM.yyyy H:mm:ss");

		supportedLocale.add("en_GB");
		longDateHM.put("en_GB","dd MMMM yyyy");
		mediumDateHM.put("en_GB","dd-MMM-yy");
		shortDateHM.put("en_GB","dd/MM/yyyy");
		longTimeHM.put("en_GB","HH:mm:ss");
		mediumTimeHM.put("en_GB","hh:mm a");
		shortTimeHM.put("en_GB","HH:mm");
		generalDateHM.put("en_GB","dd/MM/yyyy HH:mm:ss");
		
		supportedLocale.add("pt_PT");
		longDateHM.put("pt_PT","EEEE, d' de 'MMMM' de 'yyyy");
		mediumDateHM.put("pt_PT","dd-MMM-yy");
		shortDateHM.put("pt_PT","dd-MM-yyyy");
		longTimeHM.put("pt_PT","HH:mm:ss");
		mediumTimeHM.put("pt_PT","hh:mm ");
		shortTimeHM.put("pt_PT","HH:mm");
		generalDateHM.put("pt_PT","dd-MM-yyyy HH:mm:ss");
		
		supportedLocale.add("en_IN");
		longDateHM.put("en_IN","dd MMMM yyyy");
		mediumDateHM.put("en_IN","dd-MMM-yy");
		shortDateHM.put("en_IN","dd-MM-yyyy");
		longTimeHM.put("en_IN","HH:mm:ss");
		mediumTimeHM.put("en_IN","hh:mm a");
		shortTimeHM.put("en_IN","HH:mm");
		generalDateHM.put("en_IN","dd-MM-yyyy HH:mm:ss");
		
		supportedLocale.add("en_CA");
		longDateHM.put("en_CA","MMMM-dd-yy");
		mediumDateHM.put("en_CA","dd-MMM-yy");
		shortDateHM.put("en_CA","dd/MM/yyyy");
		longTimeHM.put("en_CA","h:mm:ss a");
		mediumTimeHM.put("en_CA","hh:mm a");
		shortTimeHM.put("en_CA","HH:mm");
		generalDateHM.put("en_CA","dd/MM/yyyy h:mm:ss a");
		
		supportedLocale.add("en_AU");
		longDateHM.put("en_AU","EEEE, d MMMM yyyy");
		mediumDateHM.put("en_AU","dd-MMM-yy");
		shortDateHM.put("en_AU","d/MM/yyyy");
		longTimeHM.put("en_AU","h:mm:ss a");
		mediumTimeHM.put("en_AU","hh:mm a");
		shortTimeHM.put("en_AU","HH:mm");
		generalDateHM.put("en_AU","d/MM/yyyy h:mm:ss a");

	}
	
	private static String getCode(){
		Locale l=Locale.getDefault();
		String lan=l.getLanguage();
		String h=
		  ( (l.getLanguage().length()>0)?l.getLanguage():"");
	      h+= ( (l.getCountry().length()>0)?"_"+l.getCountry():"");
	      h+=( (l.getVariant().length()>0)?"_"+l.getVariant():"");
	   
	     if( supportedLocale.contains(h)){
	    		 return h;
	     }else if(lanDefaultHM.containsKey(lan)){
	    	 return lanDefaultHM.get(lan);
	     }
	    		 else return "en_US";
	}
	
	
	public static  String getLongDatePattern() {
		return longDateHM.get(getCode());
	}
	public  static   String getMediumDatePattern() {
		return mediumDateHM.get(getCode());
	}
	public  static  String getShortDatePattern() {
		return shortDateHM.get(getCode());
	}
	public  static  String getLongTimePattern() {
	 return longTimeHM.get(getCode());
	}
	public  static  String getMediumTimePattern() {
		 return mediumTimeHM.get(getCode());
	}
	public  static String getShortTimePattern() {
		return shortTimeHM.get(getCode());
	}
	
	
	public  static String getGeneralPattern() {
		return generalDateHM.get(getCode());
	}
	
	

}
