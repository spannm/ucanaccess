package net.ucanaccess.jdbc;

import io.github.spannm.jackcess.Database;

import java.io.File;
import java.io.IOException;

public interface IJackcessOpenerInterface {
    Database open(File fl, String pwd) throws IOException;

}
