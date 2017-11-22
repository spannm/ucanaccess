/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package net.ucanaccess.jdbc;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;

public class UcanloadDriver implements Driver {
	private static Driver d;
	private static SQLException ex;
	public static final String URL_PREFIX = "jdbc:ucanaccess://";

	static {
		try {
			DriverManager.registerDriver(new UcanloadDriver());
			String home = System.getProperty("UCANACCESS_HOME");
			if (home == null) {
				final String classLocation = UcanloadDriver.class.getName()
						.replace('.', '/')
						+ ".class";
				final ClassLoader loader = UcanloadDriver.class
						.getClassLoader();
				final String classLocationString = loader.getResource(
						classLocation).toString();
				if (classLocationString.indexOf("ucanload.jar!")>0) {
					final String jarLocationString = classLocationString
							.substring(9, classLocationString.indexOf(
									"ucanload.jar!", 0));
					File jarFolder = new File(jarLocationString);
					home = jarFolder.getParent();
				}

			}
			if (home == null) {
				throw noHome();
			}

			File dir = new File(home);
			File lib = new File(home, "lib");
			if (!lib.exists()) {
				throw wrongHome();
			}
			FilenameFilter fnf = new FilenameFilter() {

				public boolean accept(File dir, String name) {
					return name.toLowerCase().endsWith(".jar");
				}
			};
			File[] ucajar = dir.listFiles(fnf);
			File[] libjars = lib.listFiles(fnf);
			List<URL> au = new ArrayList<URL>();
			for (File f : ucajar) {
				au.add(f.toURI().toURL());
			}
			for (File f : libjars) {

				au.add(f.toURI().toURL());
			}
			URL[] ucadURL = au.toArray(new URL[au.size()]);

			ClassLoader ucadLd = URLClassLoader.newInstance(ucadURL);
			d = (Driver) ucadLd.loadClass(
					"net.ucanaccess.jdbc.UcanaccessDriver").newInstance();

		} catch (Exception e) {
			ex = (SQLException) (e instanceof SQLException ? e
					: new SQLException(e));
		}
	}

	private static SQLException noHome() {
		return new SQLException(
				"The UCANACCESS_HOME system variable isn't defined:\n it should be:\n-DUCANACCESS_HOME=<your path to the UCanAccess-3.x.x-bin folder> ");
	}

	private static SQLException wrongHome() {
		return new SQLException(
				"UCANACCESS_HOME system variable doesn't point to the correct ucanaccess home\n it should be:\n -DUCANACCESS_HOME=<your path to the UCanAccess-3.x.x-bin folder>");
	}

	private static void check() throws SQLException {
		if (ex != null) {
			throw ex;
		}
		if (d == null) {
			throw wrongHome();
		}
	}

	public boolean acceptsURL(String url) throws SQLException {
		return (url.startsWith(URL_PREFIX) && url.length() > URL_PREFIX
				.length());
	}

	public Connection connect(String url, Properties pr) throws SQLException {
		check();
		return d.connect(url, pr);
	}

	public boolean equals(Object obj) {
		return d.equals(obj);
	}

	public int getMajorVersion() {
		return d.getMajorVersion();
	}

	public int getMinorVersion() {
		return d.getMinorVersion();
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties arg1)
			throws SQLException {
		return d.getPropertyInfo(url, arg1);
	}

	public boolean jdbcCompliant() {
		return d.jdbcCompliant();
	}

}
