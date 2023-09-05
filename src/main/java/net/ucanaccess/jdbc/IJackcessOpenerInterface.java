package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.Database;

import java.io.File;
import java.io.IOException;

public interface IJackcessOpenerInterface {
    Database open(File fl, String pwd) throws IOException;

}
