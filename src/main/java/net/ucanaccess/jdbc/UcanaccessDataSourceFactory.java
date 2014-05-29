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

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

public class UcanaccessDataSourceFactory implements ObjectFactory {

    public Object getObjectInstance(Object uref, Name name, Context nameCtx,
                                    Hashtable<?,?> environment) throws Exception {

        String    dataSourceClass =UcanaccessDataSource.class.getName();
        Reference ref     = (Reference) uref;
        if (ref.getClassName().equals(dataSourceClass)) {
            UcanaccessDataSource dataSource = new  UcanaccessDataSource();
            dataSource.setAccessPath((String) ref.get("accessPath").getContent());
            dataSource.setUser((String) ref.get("user").getContent());
            dataSource.setPassword((String) ref.get("password").getContent());
            return dataSource;
        } else {
            return null;
        }
    }
}
