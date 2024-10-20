/**
 * Copyright contributors to the db2-accelerator-data-comparison project
 */
package com.ibm.db2.accelerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class Db2AcceleratorDataComparison {
	
	/**
	 * Searches for the shortest key of the table. Returns the columns of the key in an ORDER BY clause format. 
	 * @param connection - to access Db2 for z/OS catalog tables
	 * @param tableSchema - schema of the table
	 * @param tableName - name of the table
	 * @return columns of the key in ORDER BY clause format (e.g. 1,3,5,7)
	 * @throws SQLException - in case the connection to Db2 for z/OS fails
	 */
	public static String getShortestUniqueKeyAsOrderByClause(Connection connection, String tableSchema, String tableName) throws SQLException {
		
		PreparedStatement findShortestKeyStatement = connection.prepareStatement("SELECT A.COLNO "
				+ "	FROM SYSIBM.SYSKEYCOLUSE A, SYSIBM.SYSTABCONST B "
				+ "	WHERE A.TBCREATOR=? AND "
				+ "	      A.TBNAME=? AND "
				+ "	      A.TBCREATOR=B.TBCREATOR AND "
				+ "	      A.TBNAME=B.TBNAME AND "
				+ "	      A.CONSTNAME = B.CONSTNAME AND "
				+ "	      A.CONSTNAME = (SELECT CONSTNAME FROM SYSIBM.SYSTABCONST WHERE TBCREATOR = ? AND TBNAME = ? ORDER BY COLCOUNT FETCH FIRST ROW ONLY) "
				+ "	ORDER BY A.COLSEQ");
		
		findShortestKeyStatement.setString(1, tableSchema);
		findShortestKeyStatement.setString(2, tableName);
		findShortestKeyStatement.setString(3, tableSchema);
		findShortestKeyStatement.setString(4, tableName);
		
		var shortestKeyResultSet = findShortestKeyStatement.executeQuery();
		
		String orderClause = "";
		
		while (shortestKeyResultSet.next()) { 
			if (!orderClause.isEmpty()) {
				orderClause += ", ";
			}
			orderClause += shortestKeyResultSet.getString(1);
		}	
		return orderClause;
	}
	
	/**
	 * Returns all columns of a table in strung integer format for a ORDER BY clause. 
	 * @param connection - connection to access Db2 for z/OS metadata
	 * @param tableSchema - schema of the table
	 * @param tableName - name of the table
	 * @return string in the format 1,2,3,4... which can be used for the ORDER BY clause
	 * @throws SQLException - in case the connection to Db2 for z/OS fails or no columns can be found
	 */
	public static String getAllColumnsOfTableForOrderByClause(Connection connection, String tableSchema, String tableName) throws SQLException { 
		
		PreparedStatement howManyColumnsStatement = connection.prepareStatement("SELECT COLCOUNT "
				+ "	FROM SYSIBM.SYSTABLES "
				+ "	WHERE CREATOR=? AND "
				+ "	      NAME=? ");
		
		howManyColumnsStatement.setString(1, tableSchema);	
		howManyColumnsStatement.setString(2, tableName);
		
		var howManyColumnsResultSet = howManyColumnsStatement.executeQuery();
		
		String orderClause = "";
		
		if (howManyColumnsResultSet.next()) {
			for (var columnNumberToProcess = 1; columnNumberToProcess <= howManyColumnsResultSet.getInt(1); columnNumberToProcess++) {	
				if (!orderClause.isEmpty()) {
					orderClause += ", ";
				}
				orderClause += columnNumberToProcess;
			}
		} else {
			throw new SQLException("No columns found");
		}
		return orderClause;
		
	}
		
	/**
	 * Return the string representation of the row. The row is being taken from the result set location. 
	 * @param resultSet The result set
	 * @return All columns in string representation, separated by | 
	 * @throws SQLException - in case the result set can not be accessed
	 */
	public static String getRowOnResultSetLocationAsString(ResultSet resultSet) throws SQLException {
		String result = "";
		for (var columnNumberToProcess = 1; columnNumberToProcess <= resultSet.getMetaData().getColumnCount(); columnNumberToProcess++) {	
			if (columnNumberToProcess > 1 ) { result += " | "; }
			result += resultSet.getString(columnNumberToProcess);
		}
		return result;
	}
	
	
	public static void printRowOnResultSetLocation(ResultSet resultSet) throws SQLException {
		System.out.println(getRowOnResultSetLocationAsString(resultSet));
	}
	
	/**
	 * Helper function. Returns the errorMessage if the stringToCheck is blank or null. 
	 * @param stringToCheck - the string to check 
	 * @param errorMessage - the error message to return
	 * @return empty string or errorMessage
	 */
	public static String ifNullOrBlankReturnErrorMessage(String stringToCheck, String errorMessage) {
		String result = "";
		if (stringToCheck == null || stringToCheck.isBlank())  {
			result = errorMessage + System.lineSeparator();
		}
		return result;
	}
	
	/**
	 * Helper function. Returns the errorMessage if the stringToCheck is not an integer number. 
	 * @param stringToCheck - the string to check 
	 * @param errorMessage - the error message to return
	 * @return empty string or errorMessage
	 */
	public static String ifNotAnInetegerReturnErrorMessage(String stringToCheck, String errorMessage) {
		String result = "";
		try {
			Integer.parseInt(stringToCheck);
		} catch (Exception e) {
			result = errorMessage;
		}
		return result;
	}
	
	/**
	 * Program main entry point
	 * @param args
	 */
	public static void main(String[] args) {
		try {	
			
			//########################################################
			//### Parsing program options and initialize variables ###
			//########################################################
			
			final Options programOptions = new Options();
			programOptions.addOption(new Option("s", "table-schema", true, "the schema of the table"));
			programOptions.addOption(new Option("n", "table-name", true, "the name of the table"));
			programOptions.addOption(new Option("u", "user", true, "the user for the connection to Db2 for z/OS"));
			programOptions.addOption(new Option("p", "password", true, "the password for the connection to Db2 for z/OS"));
			programOptions.addOption(new Option("f", "file", true, "the output file for any differences"));
			programOptions.addOption(new Option("a", "accelerator", true, "the name of the accelerator to compare with"));
			programOptions.addOption(new Option("c", "connectionUrl", true, "the Db2 connection url. Format host:port/location (e.g. 192.168.178.10:8010/DB11)"));
			programOptions.addOption(new Option("d", "debug", false, "print debug information"));
			programOptions.addOption(new Option("m", "max-differences", true, "the maximum number of differences to print before checking is stopped. Default is 100."));
								
			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(programOptions, args);
				
			String tableSchema = cmd.getOptionValue("s");
			String tableName = cmd.getOptionValue("n");
			String acceleratorName = cmd.getOptionValue("a");
			String username = cmd.getOptionValue("u");
			String password = cmd.getOptionValue("p");
			String connectionUrl = cmd.getOptionValue("c");
			boolean debugMode = cmd.hasOption("d");
			int maxNumberOfErrors = 100;
						
			String parsingErrors = "";
			parsingErrors += ifNullOrBlankReturnErrorMessage(tableSchema ,"No table schema was specified.");
			parsingErrors += ifNullOrBlankReturnErrorMessage(tableName ,"No table name was specified.");
			parsingErrors += ifNullOrBlankReturnErrorMessage(acceleratorName, "No accelerator name was specified.");
			parsingErrors += ifNullOrBlankReturnErrorMessage(username, "No user name was specified");
			parsingErrors += ifNullOrBlankReturnErrorMessage(password, "No password was specified");
			parsingErrors += ifNullOrBlankReturnErrorMessage(connectionUrl, "No connection URL was specified");
			if (cmd.hasOption("m")) {
				try {
					maxNumberOfErrors = Integer.parseInt(cmd.getOptionValue("m"));
				} catch (Exception e) {
					parsingErrors += "Maximum number of difference is not an integer number.";
				}
			}
			
			if (!parsingErrors.isEmpty()) {
				System.err.println(parsingErrors);
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("idaadiff", programOptions);
				System.exit(8);
			}
			
			Logger logger = Logger.getInstance();
			logger.setDebugMode(debugMode);
			
			//#######################################################
			//### Starting main program logic to compare the data ###
			//#######################################################
			
			Class.forName("com.ibm.db2.jcc.DB2Driver");
									
			Properties properties = new java.util.Properties();
			properties.setProperty("user", username);
			properties.setProperty("password", password);

			Connection connectionNotAccelerated = DriverManager.getConnection("jdbc:db2://" + connectionUrl, properties); 	
			Connection connectionAccelerated = DriverManager.getConnection("jdbc:db2://" + connectionUrl, properties); 
			
			logger.addLogEntryDebug("Unique key criteria: " + getShortestUniqueKeyAsOrderByClause(connectionNotAccelerated, tableSchema, tableName));
			logger.addLogEntryDebug("Full row: " + getAllColumnsOfTableForOrderByClause(connectionNotAccelerated, tableSchema, tableName));
			
			var orderByClause = getShortestUniqueKeyAsOrderByClause(connectionNotAccelerated, tableSchema, tableName);
			if (orderByClause.isEmpty()) {
				orderByClause = getAllColumnsOfTableForOrderByClause(connectionNotAccelerated, tableSchema, tableName);
			}
			
			String selectQueryString = "SELECT * FROM  " + tableSchema + "." + tableName + " ORDER BY " + orderByClause;
			
			ResultSet resultSet = connectionNotAccelerated.createStatement().executeQuery(selectQueryString);
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			
			connectionAccelerated.createStatement().execute("SET CURRENT QUERY ACCELERATION ALL");
			connectionAccelerated.createStatement().execute("SET CURRENT ACCELERATOR " + acceleratorName);	
		
			Statement acceleratedStatement = connectionAccelerated.createStatement();
			ResultSet acceleratedResultSet = acceleratedStatement.executeQuery(selectQueryString);
			
			List<String> notInDb2 = new Vector<String>();
			List<String> notInAccelerator = new Vector<String>();
			
			boolean db2Next = resultSet.next();
			boolean acceleratorNext = acceleratedResultSet.next(); 
			
			while ((db2Next || acceleratorNext) && ((notInDb2.size() + notInAccelerator.size() < maxNumberOfErrors))) { 	
				
				if (!db2Next) {
					// db2 has no rows anymore - all remaining rows will only be on the accelerator		
					logger.addLogEntryDebug("Row missing in Db2");
					logger.addLogEntryDebug(getRowOnResultSetLocationAsString(acceleratedResultSet));
					
					notInDb2.add(getRowOnResultSetLocationAsString(acceleratedResultSet));
					
					acceleratorNext = acceleratedResultSet.next();
				} else if (!acceleratorNext) {
					// accelerator has no rows anymore - all remaining rows will only be in Db2
					logger.addLogEntryDebug("Row missing in Accelerator");
					logger.addLogEntryDebug(getRowOnResultSetLocationAsString(resultSet));
					
					notInAccelerator.add(getRowOnResultSetLocationAsString(resultSet));

					db2Next = resultSet.next();
				} else {
					int compare = 0;
					for (var columnNumberToProcess = 1; columnNumberToProcess <= resultSetMetaData.getColumnCount(); columnNumberToProcess++) {	
						
						compare = resultSet.getString(columnNumberToProcess).compareTo(acceleratedResultSet.getString(columnNumberToProcess));
						
						if (compare > 0) {		
							logger.addLogEntryDebug("Row not matching (>)");
							logger.addLogEntryDebug(getRowOnResultSetLocationAsString(resultSet));
							logger.addLogEntryDebug(getRowOnResultSetLocationAsString(acceleratedResultSet));
							
							notInDb2.add(getRowOnResultSetLocationAsString(acceleratedResultSet));
						
							acceleratorNext = acceleratedResultSet.next();
							break;
						} else if (compare < 0) {
							logger.addLogEntryDebug("Row not matching (<)");
							logger.addLogEntryDebug(getRowOnResultSetLocationAsString(resultSet));
							logger.addLogEntryDebug(getRowOnResultSetLocationAsString(acceleratedResultSet));
							
							notInAccelerator.add(getRowOnResultSetLocationAsString(resultSet));

							db2Next = resultSet.next();
							break;
						}
					}
					if (compare == 0) {
						logger.addLogEntryDebug("Rows match");
						logger.addLogEntryDebug(getRowOnResultSetLocationAsString(resultSet));
						logger.addLogEntryDebug(getRowOnResultSetLocationAsString(acceleratedResultSet));
						
						acceleratorNext = acceleratedResultSet.next();
						db2Next = resultSet.next();
					}
				}
			}
			
			if (notInDb2.size() + notInAccelerator.size() == 0) {
				System.out.println("Data is the same in Db2 for z/OS and IBM Db2 Analytics Accelerator / Data Gate.");
			} else {
				System.out.println("Data is NOT in Sync. Printing the first " + maxNumberOfErrors + " differences.");
				if (notInDb2.size() > 0) {
					System.out.println("Not in Db2 for z/OS are the following rows:");
					notInDb2.forEach(row -> { System.out.println(row);});
				}
				if (notInAccelerator.size() > 0) {
					System.out.println("Not on the accelerator are the following rows:");
					notInAccelerator.forEach(row -> {System.out.println(row);});
				}
				System.exit(8);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
