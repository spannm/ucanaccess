package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import net.ucanaccess.util.Try;

import java.io.File;
import java.io.IOException;

public class DefaultJackcessOpener implements IJackcessOpenerInterface {

    @Override
    public Database open(File fl, String pwd) throws IOException {
        DatabaseBuilder dbd = new DatabaseBuilder(fl).setAutoSync(false);
        Database db = Try.catching(() -> {
            return dbd.setReadOnly(false).open();
        }).orElseGet(() -> {
            return dbd.setReadOnly(true).open();
        });
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        return db;
    }

}
