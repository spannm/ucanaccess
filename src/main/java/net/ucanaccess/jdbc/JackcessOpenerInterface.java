package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.Database;

import java.io.File;
import java.io.IOException;

public interface JackcessOpenerInterface {
    Database open(File fl, String pwd) throws IOException;

}
