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
package net.ucanaccess.console;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

import net.ucanaccess.util.Logger;

public class Main {
	private static final String EXPORT_USAGE = "export [-d <delimiter>] [-t <table>] <pathToCsv>";

	private static final String EXPORT_PROMPT = "Export command syntax is: " + EXPORT_USAGE;	
		
	private static final String DEFAULT_CSV_DELIMITER = ";";
	
	private static boolean batchMode=false;
	private Connection conn;
	private boolean connected = true;
	private BufferedReader input;
	private String lastSqlQuery;
	
	public Main(Connection conn, BufferedReader input) {
		this.conn = conn;
		this.input = input;
		
	}
	
	private static boolean hasPassword(File fl) throws IOException {
		Database db;
		try{
		db = DatabaseBuilder.open(fl);
	      }catch(IOException e){
	    	  DatabaseBuilder dbb=new  DatabaseBuilder();
	    	  dbb.setReadOnly(true);
	    	  dbb.setFile(fl);
		      db= dbb.open();
		
	    }
		String pwd = db.getDatabasePassword();
		db.close();
		return pwd != null;
	}
	
	private static void lcProperties(Properties pr) {
		Properties nb=new Properties();
		
		for( Entry<Object, Object> entry:pr.entrySet()){
			String key=(String)entry.getKey();
			if(key!=null){
				nb.put(key.toLowerCase(), entry.getValue());
			}
		}
		pr.clear();
		pr.putAll(nb);
	}
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Logger.setLogPrintWriter(new PrintWriter(System.out));
		BufferedReader input = new BufferedReader(new InputStreamReader(
				System.in));
		// password properties info
		Properties info=new Properties();
		if(args.length>0){
			File pfl =new File(args[0]);
			if(pfl.exists()){
				FileInputStream fis=new FileInputStream (pfl);
				info.load(fis);
				lcProperties(info);
			}
		}
		
		try {
			Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		} catch (ClassNotFoundException e) {
		
			System.out.println(e.getMessage());
			System.out.println("Check your classpath! ");
			System.exit(1);
		}
		Connection conn = null;
		File fl = null;
		long size=0;
		while (fl == null || !fl.exists()) {
			if (fl != null) {
				System.out.println("Given file does not exist");
			}
			System.out.print("Please, enter the full path to the access file (.mdb or .accdb): ");
			String path = input.readLine().trim();
			if (path.endsWith(";"))
				path = path.substring(0, path.length() - 1);
			if(path.equalsIgnoreCase("quit")){
				System.out.println("I'm so unhappy. Goodbye.");
				System.exit(1);
			}
			fl = new File(path);
			size=fl.length();
		}
		try {
			String passwordEntry = "";
			String noMem="";
			if (info.containsKey("jackcessopener")||hasPassword(fl)) {
				System.out.print("Please, enter password: ");
				passwordEntry = ";password=" + input.readLine().trim();
			}
			
			if(!info.containsKey("jackcessopener"))
			noMem=size>30000000?";memory=false":"";
			
			conn = DriverManager.getConnection("jdbc:ucanaccess://"
					+ fl.getAbsolutePath() + passwordEntry+noMem,info
					);
			
			SQLWarning sqlw= conn.getWarnings();
			while(sqlw!=null){
				System.out.println(sqlw.getMessage());
				sqlw=sqlw.getNextWarning();	
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			System.exit(1);
		}
		Main main = new Main(conn, input);
		main.sayHello(conn.getMetaData().getDriverVersion());
		main.start();
	}
	
	public static void setBatchMode(boolean batchMode) {
		Main.batchMode = batchMode;
	}

	/**
	 * Prints the ResultSet {@code rs} in a format suitable for the terminal 
	 * console given by {@code out}.
	 */
	public void consoleDump(ResultSet rs, PrintStream out)
			throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int cols = meta.getColumnCount();

		// Print the header.
		StringBuilder header = new StringBuilder("| ");
		for (int i = 1; i <= cols; ++i) {
			header.append(meta.getColumnLabel(i));
			header.append(" | ");
		}
		StringBuilder interline = new StringBuilder();
		for (int i = 0; i < header.length(); ++i) {
			interline.append("-");
		}
		out.println(interline);
		out.println(header);
		out.println(interline);
		out.println();
		
		// Print the result set.
		while (rs.next()) {
			System.out.print("| ");
			for (int i = 1; i <= cols; ++i) {
				Object o = rs.getObject(i);
				if(o!=null&&o.getClass().isArray()){
					o=Arrays.toString((Object[])o);
				}
				
				out.print(o + " | ");
			}
			out.println();
			out.println();
		}
	}
	
	/**
	 * Prints the ResultSet {@code rs} in CSV format using the {@code delimiter} to the
	 * output file {@code out}.
	 */
	public void csvDump(ResultSet rs, String delimiter, PrintStream out)
			throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int cols = meta.getColumnCount();
		
		// Print the CSV header row.
		String comma = "";
		for (int i = 1; i <= cols; ++i) {
			String lb=meta.getColumnLabel(i);
			out.print(comma);
			out.print(toCsv(lb, delimiter));
			comma = delimiter;
		}
		out.println();
		
		// Print the result set rows.
		while (rs.next()) {
			comma = "";
			for (int i = 1; i <= cols; ++i) {
				Object o = rs.getObject(i);
				if(o==null)o="";
				if(o!=null&&o.getClass().isArray()){
					o=Arrays.toString((Object[])o);
				}
				if(o instanceof Date){
					SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					o=df.format((Date)o);
				}
				if(o instanceof BigDecimal){
					DecimalFormat df = new DecimalFormat("0.0########");
					DecimalFormatSymbols dfs=new DecimalFormatSymbols();
					dfs.setDecimalSeparator('.');
					df.setDecimalFormatSymbols(dfs);
					df.setGroupingUsed(false);
					o=df.format(o);
					
				}
				out.print(comma);
				out.print(toCsv(o.toString(), delimiter));
				comma = delimiter;
			}
			out.println();
			
		}
	}
	
	/**
	 * Returns the CSV representation of the string {@code s}.
	 * <ul>
	 * <li> double-quote characters (") are doubled (""), and then enclosed in double-quotes
	 * <li> if the string contains the delimiter character, wrap the string in double-quotes
	 * <li> replace newline character with the space character
	 * </li>
	 * This supports only a small subset of various CSV transformations such as those given in  
	 * https://www.csvreader.com/csv_format.php.
	 * 
	 * <p>TODO: Consider using a 3rd party formatter like {@code org.apache.commons.csv.CSVFormat}
	 * if we don't mind adding another dependency.
	 */
	private static String toCsv(String s, String delimiter) {
		boolean needsTextQualifier = false;
		
		// A double-quote is replaced with 2 double-quotes.
		if (s.contains("\"")) {
			s = s.replace("\"",  "\"\"");
			needsTextQualifier = true;
		}
		
		// If the string contains the delimiter, then we must wrap it in quotes.
		if (s.contains(delimiter)) {
			needsTextQualifier = true;
		}
		
		// Newlines are replaced with spaces. 
		// TODO(btpark): Add an option to preserve newline characters.
		s = s.replace("\n", " ");
		
		if (needsTextQualifier) {
			return "\"" + s + "\"";
		} else {
			return s;
		}
	}
	
	private void executeStatement(String sql) throws SQLException {
		Statement st = conn.createStatement();
		try {
			if (st.execute(sql)) {
				ResultSet rs = st.getResultSet();
				if (rs != null) {
					consoleDump(rs, System.out);
					this.lastSqlQuery = sql;
				} else {
					System.out.println("Ok!");
				}
			} else {
				int num = st.getUpdateCount();
				prompt(num == 0 ? "No rows affected" : num + " row(s) affected");
			}
		} finally {
			st.close();
		}
	}

	private void prompt() {
		System.out.println();
		if (!batchMode)
			System.out.print("UCanAccess>");
	}
	
	private void prompt(String content) {
		if (!batchMode)
			System.out.println("UCanAccess>" + content);
	}
	
	private String readInput() {
		try {
			String ret = input.readLine();
			if (ret == null) {
				prompt("Ciao!");
				System.exit(0);
			}
			return ret.trim();
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	private void sayHello(String version) {
		prompt("");
		System.out.printf("Copyright (c) %d Marco Amadei%n", java.util.Calendar.getInstance().get(java.util.Calendar.YEAR));
		System.out.println("UCanAccess version "+version);
		System.out.println("You are connected!! ");
		
		System.out.println("Type quit to exit ");
		System.out.println();
		System.out.println("Commands end with ; ");
		System.out.println();
		System.out.println("Use:   ");
		System.out.printf("   %s;%n", EXPORT_USAGE);
		System.out.println("for exporting the result set from the last executed query or a specific table into a .csv file");
		prompt();
		
	}
	
	private void start() {
		StringBuilder sb = new StringBuilder();
		while (connected) {
			String userInput = readInput();
			if (userInput.equalsIgnoreCase("quit")) {
				connected = false;
				break;
			}
			sb.append(" ").append(userInput);

			// If the current userInput ends with ';', then execute the buffered command.
			if (userInput.endsWith(";")) {
				String cmd = sb.toString().substring(0, sb.length() - 1).trim();
				try {
					if (cmd.toLowerCase().startsWith("export ")) {
						executeExport(cmd);
					} else {
						executeStatement(cmd);
					}
				} catch (Exception e) {
					prompt(e.getMessage());
				}
				sb = new StringBuilder();
				this.prompt();
			}
		}
		System.out.println("Cheers! Thank you for using the UCanAccess JDBC Driver.");
	}

	/**
	 * Parse the {@code cmd} to handle command line flags of the form:
	 * "export [-d delimiter] [-t table] pathToCsv". For example:
	 * <pre>
	 * export -d , -t License License.csv
	 * </pre>
	 * The {@code -d ,} option changes the delimiter character to a comma instead of the 
	 * default semicolon.
	 * The {@code -t table} option dumps the {@code License} table using the SQL statement
	 * "select * from [License]".
	 */
	private void executeExport(String cmd) throws SQLException, FileNotFoundException {
		List<String> tokens = tokenize(cmd);

		String delimiter = DEFAULT_CSV_DELIMITER;
		String table = null;

		// Process the command line flags.
		// TODO: Consider using a 3rd party command line argument parser.
		int i = 1; // skip the first token which will always be "export"
		for (; i < tokens.size(); i++) {
			String arg = tokens.get(i);
			if (!arg.startsWith("-")) {
				break;
			}
			if ("-d".equals(arg)) {
				++i;
				if (i >= tokens.size()) {
					prompt("Missing parameter for -d flag");
					prompt(EXPORT_PROMPT);
					return;
				}
				delimiter = tokens.get(i);
			} else if ("-t".equals(arg)) {
				++i;
				if (i >= tokens.size()) {
					prompt("Missing parameter for -t flag");
					prompt(EXPORT_PROMPT);
					return;
				}
				table = tokens.get(i);
			} else {
				prompt("Unknown flag " + arg);
				prompt(EXPORT_PROMPT);
				return;
			}
			
		}
		if (i >= tokens.size()) {
			prompt("File name not found");
			prompt(EXPORT_PROMPT);
			return;
		}
		if (i < tokens.size() - 1) {
			prompt("Too many arguments");
			prompt(EXPORT_PROMPT);
			return;
		}
		String fileName = tokens.get(i);
		
		// Determine the SQL statement to execute. If the '-t table' option is given
		// run a 'select * from [table]' query to export the table, instead of 
		// executing the 'lastSqlQuery'.
		String sqlQuery;
		if (table != null && !table.isEmpty()) {
			sqlQuery = "select * from [" + table + "]";
		} else if (lastSqlQuery != null) {
			sqlQuery = lastSqlQuery;
		} else {
			prompt("You must first execute an SQL query, then export the ResultSet!");
			return;
		}
		
		// Run the SQL statement and write to fileName.
		Statement statement = conn.createStatement();
		try {
			ResultSet rs = statement.executeQuery(sqlQuery);
			File fl = new File(fileName);
			PrintStream out = new PrintStream(fl);
			try {
				csvDump(rs, delimiter, out);
				out.flush();
			} finally {
				out.close();
			}
			prompt("Created file: " + fl.getAbsolutePath());
		} finally {
			statement.close();
		}
	}
	
	// TODO: Consider using a smarter tokenizer that knows how to handle quoted strings.
	// Maybe StreamTokenizer.
	static List<String> tokenize(String s) {
		StringTokenizer st = new StringTokenizer(s);
		List<String> tokens = new ArrayList<String>(st.countTokens());
		while (st.hasMoreTokens()) {
			tokens.add(st.nextToken());
		}
		return tokens;
	}
}
