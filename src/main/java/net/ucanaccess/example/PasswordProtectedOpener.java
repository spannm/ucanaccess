package net.ucanaccess.example;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.encrypt.CryptCodecProvider;
import net.ucanaccess.jdbc.IJackcessOpenerInterface;

import java.io.File;
import java.io.IOException;

/**
 * Custom opener for encrypted databases.
 * <p>
 * Required by UcanAccess to handle password-protected files via Jackcess.
 */
public final class PasswordProtectedOpener implements IJackcessOpenerInterface {

    @Override
    public Database open(File file, String pwd) throws IOException {
        return new DatabaseBuilder()
            .withFile(file)
            .withCodecProvider(new CryptCodecProvider(pwd))
            .open();
    }

}
