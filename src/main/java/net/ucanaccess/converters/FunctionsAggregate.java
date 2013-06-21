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

import java.math.BigDecimal;
import java.sql.Timestamp;


public class FunctionsAggregate {
	public static  Object first( Object  in, Boolean flag,
			Object [] register, Integer[] counter) {
       if (flag) {
          return register[0];
       }
       if( register[0]==null)
       register[0] = in;
       if(counter[0]==null)counter[0]=0;
       counter[0] = counter[0] + 1;
       
       return null;
   }
	
	
	public static  BigDecimal first( BigDecimal  in, Boolean flag,
			 BigDecimal [] register, Integer[] counter) {
       return (BigDecimal)
       first( (Object)  in, flag,
			(Object[]) register,  counter);
		
	
    }
		
	public static String first(String in, Boolean flag,
            String[] register, Integer[] counter) {
		return (String)
	       first( (Object)  in, flag,
				(Object[]) register,  counter);
    }
		
	public static Boolean first(Boolean in, Boolean flag,
			Boolean[] register, Integer[] counter) {
		return (Boolean)
	       first( (Object)  in, flag,
				(Object[]) register,  counter);
    }
	
	public static Timestamp first(Timestamp in, Boolean flag,
			Timestamp[] register, Integer[] counter) {
		return (Timestamp)
	       first( (Object)  in, flag,
				(Object[]) register,  counter);
    }
	
		
	
	

	public static  Object last( Object  in, Boolean flag,
			Object [] register, Integer[] counter) {
       if (flag) {
          return register[0];
       }
       register[0] = in;
       if(counter[0]==null)counter[0]=0;
       counter[0] = counter[0] + 1;
       
       return null;
   }
	public static  BigDecimal last( BigDecimal  in, Boolean flag,
			 BigDecimal [] register, Integer[] counter) {
      return (BigDecimal)
      last( (Object)  in, flag,
			(Object[]) register,  counter);
		
	
   }
	
	public static String last(String in, Boolean flag,
            String[] register, Integer[] counter) {
		return (String)
	       last( (Object)  in, flag,
				(Object[]) register,  counter);
    }

	public static Boolean last(Boolean in, Boolean flag,
			Boolean[] register, Integer[] counter) {
		return (Boolean)
	       last( (Object)  in, flag,
				(Object[]) register,  counter);
    }
	
	public static Timestamp last(Timestamp in, Boolean flag,
			Timestamp[] register, Integer[] counter) {
		return (Timestamp)
	       last( (Object)  in, flag,
				(Object[]) register,  counter);
    }
}
