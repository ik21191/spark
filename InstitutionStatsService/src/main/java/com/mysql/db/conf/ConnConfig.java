package com.mysql.db.conf;

public class ConnConfig {

	private String ipHost= "127.0.0.1";
	private int port = 3306 ;
	private String database = "";
	private String table = "";
	private String user = "";
	private String password = "";

	/**
	 * @return the ipHost
	 */
	public String getIpHost() {
		return ipHost;
	}
	/**
	 * @param ipHost the ipHost to set
	 */
	public void setIpHost(String ipHost) {
		this.ipHost = ipHost;
	}
	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}
	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}
	/**
	 * @return the database
	 */
	public String getDatabase() {
		return database;
	}
	/**
	 * @param database the database to set
	 */
	public void setDatabase(String database) {
		this.database = database;
	}
	/**
	 * @return the table
	 */
	public String getTable() {
		return table;
	}
	/**
	 * @param table the table to set
	 */
	public void setTable(String table) {
		this.table = table;
	}
	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}
	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}
	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}
	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	
	
	
}
