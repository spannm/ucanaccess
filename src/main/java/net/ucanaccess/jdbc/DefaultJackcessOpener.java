package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;

import java.io.File;
import java.io.IOException;

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
