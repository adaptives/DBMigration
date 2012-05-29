package com.diycomputerscience.dbtools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;




public class MigrationToolkit {

	private static String srcUrl = "jdbc:hsqldb:hsql://localhost:9001/diycs";
	private static String srcUsername = "sa";
	private static String srcPassword = "";
	private static String srcDriver = "org.hsqldb.jdbcDriver";
	private static String destUrl = "jdbc:mysql://localhost:3306/diycs";
	private static String destUsername = "root";
	private static String destPassword = "";
	private static String destDriver = "com.mysql.jdbc.Driver";
	
	private static String tableMigrationOrderFileName = "";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {		
		MigrationToolkit toolkit = new MigrationToolkit();
		MigrationDatabase srcDB = new MigrationDatabase(srcUrl, srcUsername, srcPassword,srcDriver);
		MigrationDatabase destDB = new MigrationDatabase(destUrl, destUsername, destPassword, destDriver);
		toolkit.run(srcDB, destDB);
		
	}
	
	public void run(MigrationDatabase srcDB, MigrationDatabase destDB) {
		List<String> srcTableNames = getTableNamesFromPropertiesFile();//getTableNamesFromMetaData(srcDB);
		for(String tableName : srcTableNames){
			try {
				migrateTable(tableName, srcDB,destDB);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Migration of table " + tableName + " failed!");
			}
		}
		
	}

	private void migrateTable(String tableName, MigrationDatabase srcDB,	MigrationDatabase destDB){
		
		
		String insertQuery = getInsertQueryForTable(tableName, srcDB);
		if (insertQuery == null){
			System.out.println("Insert query could not be generated for destination database table " + tableName);
			return;
		}
		
		
		PreparedStatement destStmt =  null; 
		PreparedStatement srcStmt = null; 
		ResultSet srcRS = null;
		ResultSetMetaData srcRSMD = null;
		long rowCount = 0;
		try{
			destStmt = destDB.getConnection().prepareStatement(insertQuery);
			srcStmt = srcDB.getConnection().prepareStatement("SELECT * FROM " + tableName);
			srcRS = srcStmt.executeQuery();
			srcRSMD = srcRS.getMetaData();
			System.out.println("Migrating table "+ tableName + " ...");
			while(srcRS.next()){
				int colCount = srcRSMD.getColumnCount();
				for(int col=1;col<=colCount;col++){
					if (srcRS.getString(col)==null){
						destStmt.setNull(col, srcRSMD.getColumnType(col));
						continue;
					}
					switch (srcRSMD.getColumnType(col)){
						case java.sql.Types.BIGINT :  
							destStmt.setLong(col,srcRS.getLong(col)); 
							break;
						case java.sql.Types.SMALLINT :
							destStmt.setShort(col,srcRS.getShort(col));
							break;
						case java.sql.Types.TINYINT :
							destStmt.setByte(col,srcRS.getByte(col));
							break;	
						case java.sql.Types.INTEGER :
							destStmt.setInt(col,srcRS.getInt(col)); 
							break;
						case java.sql.Types.DATE :
							destStmt.setDate(col,srcRS.getDate(col));
							break;
						case java.sql.Types.DOUBLE : case java.sql.Types.NUMERIC :
							destStmt.setDouble(col,srcRS.getDouble(col));
							break;
						case java.sql.Types.VARCHAR : case java.sql.Types.LONGVARCHAR : 
							destStmt.setString(col,srcRS.getString(col)); 
							break;
						case java.sql.Types.BOOLEAN : 
							destStmt.setBoolean(col,srcRS.getBoolean(col)); 
							break;
						case java.sql.Types.TIMESTAMP :
							destStmt.setTimestamp(col,srcRS.getTimestamp(col)); 
							break;
						case java.sql.Types.VARBINARY : case java.sql.Types.LONGVARBINARY : 
							destStmt.setBytes(col,srcRS.getBytes(col)); 
							break;
					
					}
				}
				destStmt.executeUpdate();
				rowCount++;
			}
			
			System.out.println("Migrating table "+ tableName + " transferred "+ rowCount + " records.");
			System.out.println("Migrating table "+ tableName + " done!");
		} catch (SQLException excp){
			
			excp.printStackTrace();
			System.out.println("Migrating table "+ tableName + " failed! at row "+ rowCount);
		} catch (Exception excp){
			excp.printStackTrace();
			System.out.println("Migrating table "+ tableName + " failed! at row "+ rowCount);
		}finally{
			try {
				if (srcRS != null){
					srcRS.close();
				}
				if (destStmt != null){
					destStmt.close();
				}
				if (srcStmt != null){
					srcStmt.close();
				}
				
			} catch (SQLException e) {
				
			}
		}
	}

	private String getInsertQueryForTable(String tableName,	MigrationDatabase srcDB) {
		
		StringBuffer insertQuery = new StringBuffer("INSERT INTO " + tableName);
		PreparedStatement stmt;
		try {
			stmt = srcDB.getConnection().prepareStatement("SELECT * FROM " + tableName);
			ResultSet rs = stmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int numCols = rsmd.getColumnCount();
			StringBuffer colNames = new StringBuffer(" ( ");
			StringBuffer colValues = new StringBuffer(" VALUES (");
			
				for(int i=1;i<=numCols;i++){
					colNames.append(rsmd.getColumnName(i));
					colValues.append(" ? ");
					if (i<numCols){
						colNames.append(" , ");
						colValues.append(" , ");
					}else{
						colNames.append(" ) ");
						colValues.append(" ) ");
					}
				}
			
				return insertQuery.append(colNames.toString()).append(colValues.toString()).toString();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	private List<String> getTableNamesFromMetaData(MigrationDatabase db){
		List<String> tablenames = new ArrayList<String>();
		try {
			Connection conn = db.getConnection();
			DatabaseMetaData metadata = conn.getMetaData();
			System.out.println(metadata.getDatabaseProductName());
			ResultSet tableRS = metadata.getTables(null, "PUBLIC", null, new String[]{"TABLE"});
			while(tableRS.next()){
				tablenames.add(tableRS.getString("TABLE_NAME"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tablenames;
	}

	private List<String> getTableNamesFromPropertiesFile(){
		List<String> tables = new ArrayList<String>();
		File tablenames = new File(tableMigrationOrderFileName);
		try {
			FileReader reader = new FileReader(tablenames);
			BufferedReader buffreader = new BufferedReader(reader);
			String tname;
			while((tname = buffreader.readLine())!=null){
				tname = tname.trim();
				if(tname.length()>0 && !tname.startsWith("#")){
					tables.add(tname);
				}	
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return tables;
	}
}
