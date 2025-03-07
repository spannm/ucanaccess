package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public interface IJackcessOpenerInterface {

    Database open(File fl, String pwd) throws IOException;

    default Database open(File fl, String pwd, Charset charset) throws IOException {
        return open(fl, pwd);
    }

}
