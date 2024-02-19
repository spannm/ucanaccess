package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.DateTimeType;
import net.ucanaccess.util.Try;

import java.io.File;
import java.io.IOException;

public class DefaultJackcessOpener implements IJackcessOpenerInterface {

    @Override
    public Database open(File fl, String pwd) throws IOException {
        DatabaseBuilder dbd = new DatabaseBuilder(fl).withAutoSync(false);
        Database db = Try.catching(() -> {
            return dbd.withReadOnly(false).open();
        }).orElseGet(() -> dbd.withReadOnly(true).open());
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        return db;
    }

}
