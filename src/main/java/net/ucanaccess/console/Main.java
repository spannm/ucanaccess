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
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

import net.ucanaccess.util.Logger;

public class Main {
	private static final String EXPORT_USAGE =
			"export [--help] [--bom] [-d <delimiter>] [-t <table>] <pathToCsv>";

	private static final String EXPORT_PROMPT = "Export command syntax is: " + EXPORT_USAGE;
			
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
	
	private void printExportHelp() {
		System.out.printf("Usage: %s;%n", EXPORT_USAGE);
		System.out.println("Export the most recent SQL query to the given <pathToCsv> file.");
		System.out.println("  -d <delimiter> Set the CSV column delimiter (default: ';')");
		System.out.println("  -t <table>     Output the <table> instead of the previous query.");
		System.out.println("  --bom          Output the UTF-8 byte order mark.");
		System.out.println("  --help         Print this help message.");
		System.out.println("Single (') or double (\") quoted strings are supported.");
		System.out.println("Backslash (\\) escaping (e.g. \\n, \\t) is enabled within quotes.");
		System.out.println("Use two backslashes (\\\\) to insert one backslash within quotes "
				+ "(e.g. \"c:\\\\temp\\\\newfile.csv\").");
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
	 * The {@code -t License} option dumps the {@code License} table using the SQL statement
	 * "select * from [License]".
	 */
	private void executeExport(String cmd) throws SQLException, FileNotFoundException, IOException {
		List<String> tokens = tokenize(cmd);

		Exporter.Builder exporterBuilder = new Exporter.Builder();
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
				exporterBuilder.setDelimiter(tokens.get(i));
			} else if ("-t".equals(arg)) {
				++i;
				if (i >= tokens.size()) {
					prompt("Missing parameter for -t flag");
					prompt(EXPORT_PROMPT);
					return;
				}
				table = tokens.get(i);
			} else if ("--bom".equals(arg)) {
				exporterBuilder.includeBom(true);
			} else if ("--help".equals(arg)) {
				printExportHelp();
				return;
			} else if ("--".equals(arg)) {
				++i;
				break;
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
		Exporter exporter = exporterBuilder.build();
		Statement statement = conn.createStatement();
		try {
			ResultSet rs = statement.executeQuery(sqlQuery);
			File fl = new File(fileName);
			PrintStream out = new PrintStream(fl);
			try {
				exporter.csvDump(rs, out);
				out.flush();
			} finally {
				out.close();
			}
			prompt("Created file: " + fl.getAbsolutePath());
		} finally {
			statement.close();
		}
	}
	

	static List<String> tokenize(String s) throws IOException {
		// Create a tokenizer that creates tokens delimited by whitespace. Also handles single and
		// double quotes. Does not support comments or numbers. All characters between U+0000
		// U+0020 are considered to be whitespace characters (this includes \r, \n, and \t).
		// All characters from U+0021 to U+00FF are normal word characters.
		// Internally, StreamTokenzier treats all characters >= U+0100 to be a normal 
		// word character, and unfortunately, this cannot be changed. 
		//
		// Non-breaking-space character U+00A0 is currently treated as a normal character,
		// but I could be convinced that it should be considered a whitespace.
		StreamTokenizer st = new StreamTokenizer(new StringReader(s));
		st.resetSyntax();
		st.wordChars(33, 255);
		st.whitespaceChars(0, ' ');
		st.quoteChar('"');
		st.quoteChar('\'');

		List<String> tokens = new ArrayList<String>();
		while (true) {
			int ttype = st.nextToken();
			if (ttype == StreamTokenizer.TT_EOF) {
				break;
			} else if (ttype == StreamTokenizer.TT_WORD) {
				tokens.add(st.sval);
			} else if (ttype == '\'' || ttype == '"') {
				tokens.add(st.sval);
			}
		}
		return tokens;
	}
}
