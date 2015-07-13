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
