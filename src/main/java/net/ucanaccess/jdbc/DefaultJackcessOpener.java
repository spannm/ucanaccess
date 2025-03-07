package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.DateTimeType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class DefaultJackcessOpener implements IJackcessOpenerInterface {

    @Override
    public Database open(File _fl, String _pwd) throws IOException {
        return open(_fl, _pwd, null);
    }

    @Override
    public Database open(File fl, String pwd, Charset charset) throws IOException {
        DatabaseBuilder dbd = new DatabaseBuilder()
            .withFile(fl)
            .withAutoSync(false)
            .withCharset(charset);

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
