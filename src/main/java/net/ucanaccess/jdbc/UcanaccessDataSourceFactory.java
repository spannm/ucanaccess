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
