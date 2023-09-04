package net.ucanaccess.jdbc;

import java.io.File;
import java.io.IOException;

import com.healthmarketscience.jackcess.Database;

public interface JackcessOpenerInterface {
    Database open(File fl, String pwd) throws IOException;

}
