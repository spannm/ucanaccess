package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.DateTimeType;

import java.io.File;
import java.io.IOException;

public class DefaultJackcessOpener implements IJackcessOpenerInterface {

    @Override
    public Database open(File fl, String pwd) throws IOException {
        DatabaseBuilder dbd = new DatabaseBuilder().withFile(fl).withAutoSync(false);
        Database db;
        try {
            db = dbd.withReadOnly(false).open();
        } catch (Exception _ex) {
            db = dbd.withReadOnly(true).open();
        }
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        return db;
    }

}
