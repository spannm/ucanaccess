package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.DateTimeType;
import io.github.spannm.jackcess.impl.CodecProvider;
import io.github.spannm.jackcess.impl.UnsupportedCodecException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;

/**
 * Opens Access databases with Jackcess. Encrypted databases are opened with
 * <a href="https://github.com/spannm/jackcess-encrypt/">Jackcess Encrypt</a> if it is on the classpath together with
 * Bouncy Castle (since 5.3.1).
 */
public class DefaultJackcessOpener implements IJackcessOpenerInterface {

    private static final String CRYPT_CODEC_PROVIDER = "io.github.spannm.jackcess.encrypt.CryptCodecProvider";
    private static final String BOUNCY_CASTLE        = "org.bouncycastle.crypto.InvalidCipherTextException";

    @Override
    public Database open(File fl, String pwd) throws IOException {
        return open(fl, pwd, null);
    }

    @Override
    public Database open(File fl, String pwd, Charset charset) throws IOException {
        DatabaseBuilder dbd = new DatabaseBuilder()
            .withFile(fl)
            .withAutoSync(false)
            .withCharset(charset);
        Optional<CodecProvider> codecProvider = cryptCodecProvider(pwd);
        codecProvider.ifPresent(dbd::withCodecProvider);

        Database db;
        try {
            db = open(dbd);
        } catch (UnsupportedCodecException ex) {
            if (codecProvider.isPresent()) {
                throw ex;
            }
            String missing = isOnClasspath(CRYPT_CODEC_PROVIDER) ? "Bouncy Castle" : "Jackcess Encrypt and Bouncy Castle";
            throw new UnsupportedCodecException(
                String.format("The database is encrypted, opening it requires %s on the classpath", missing), ex);
        }
        db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
        return db;
    }

    /**
     * Opens the database for writing, or read-only if that fails.
     */
    private static Database open(DatabaseBuilder dbd) throws IOException {
        try {
            return dbd.withReadOnly(false).open();
        } catch (Exception ex) {
            return dbd.withReadOnly(true).open();
        }
    }

    /**
     * Creates the codec provider of Jackcess Encrypt for the given password, if Jackcess Encrypt and Bouncy Castle are
     * on the classpath.
     */
    private static Optional<CodecProvider> cryptCodecProvider(String pwd) {
        if (!isOnClasspath(CRYPT_CODEC_PROVIDER) || !isOnClasspath(BOUNCY_CASTLE)) {
            return Optional.empty();
        }
        try {
            Class<?> clazz = Class.forName(CRYPT_CODEC_PROVIDER);
            return Optional.of((CodecProvider) clazz.getConstructor(String.class).newInstance(pwd));
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException(String.format("Cannot create %s", CRYPT_CODEC_PROVIDER), ex);
        }
    }

    /**
     * Returns whether the class with the given name is on the classpath.
     */
    private static boolean isOnClasspath(String className) {
        try {
            Class.forName(className, false, DefaultJackcessOpener.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException ex) {
            return false;
        }
    }

}
