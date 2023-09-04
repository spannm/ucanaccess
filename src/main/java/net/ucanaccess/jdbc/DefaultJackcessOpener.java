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

import java.io.File;
import java.io.IOException;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;

public class DefaultJackcessOpener implements JackcessOpenerInterface {

    @Override
    public Database open(File fl, String pwd) throws IOException {
        DatabaseBuilder dbd = new DatabaseBuilder(fl);
        dbd.setAutoSync(false);
        Database db = null;
        try {
            dbd.setReadOnly(false);
            db = dbd.open();
        } catch (IOException e) {
            dbd.setReadOnly(true);
            db = dbd.open();
        }
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        return db;
    }

}
