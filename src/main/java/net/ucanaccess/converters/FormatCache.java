package net.ucanaccess.converters;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Hashtable;


public class FormatCache {
	private static DecimalFormat noArgs;
	private static DecimalFormat zpzz;
	private static DecimalFormat sharp;
	private static DecimalFormat noGrouping;
	
    private static Hashtable<String, DecimalFormat> ht=new Hashtable<String, DecimalFormat>();
	
	public static DecimalFormat  getDecimalFormat(String s){
		if(!ht.containsKey(s)){
			DecimalFormat dc= new DecimalFormat(s);
			dc.setRoundingMode(RoundingMode.HALF_UP);
			ht.put(s,dc);
		}
		return ht.get(s);
	} 
	
	public static DecimalFormat  getNoArgs(){
		if(noArgs==null){
			noArgs=new DecimalFormat();
		}
		return noArgs;
	}
	public static DecimalFormat  getZpzz(){
		if(zpzz==null){
			zpzz=new DecimalFormat("0.00");
			zpzz.setRoundingMode(RoundingMode.HALF_UP);
		}
		return zpzz;
	}
	
	public static DecimalFormat  getSharp(){
		if(sharp==null){
			sharp= new DecimalFormat("###,###.##");
		}
		return sharp;
	}
	
	public static DecimalFormat  getNoGrouping(){
		if(noGrouping==null){
			noGrouping= new DecimalFormat();
			noGrouping.setGroupingUsed(false);
		}
		return noGrouping;
	}
}
