package com.diycomputerscience.dbtools;

import java.sql.Connection;
import java.sql.DriverManager;

public class MigrationDatabase {
	private String url = null;
	private String user = null;
	private String password = null;
	private String driver = null;
	private Connection conn = null;
	
	
	public MigrationDatabase(String url, 
							 String user, 
							 String password,
							 String driver) {
		super();
		this.url = url;
		this.user = user;
		this.password = password;
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}
	
	public Connection getConnection() throws Exception{
		if (conn == null || conn.isClosed()){
			if (driver == null){
				throw new Exception("No jdbc driver specified. Cannot create connection.");
			}
			Class.forName(driver).newInstance();
			conn =  DriverManager.getConnection(url,user,password);
		}
		return conn;
	}
	
}
