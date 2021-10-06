package com.mysql.db.conf;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.mps.commons.AppConfig;
import com.mps.utils.MyException;
import com.mps.utils.MyLogger;

public class MySqlConnConf extends ConnConfig implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String driver = "com.mysql.jdbc.Driver";
	private String mysqlURL;
	private int mysqlPort;
	private String mysqlUser;
	private String mysqlPass;
	private String mysqlDatabase;
	
	
	public MySqlConnConf() throws MyException {
		initialize();
		setIpHost(mysqlURL);
		setPort(mysqlPort);
		setUser(mysqlUser);
		setPassword(mysqlPass);
		setDatabase(mysqlDatabase);
	}

	/**
	 * @return the driver
	 */
	public String getDriver() {
		return driver;
	}

	/**
	 * @param driver the driver to set
	 */
	public void setDriver(String driver) {
		this.driver = driver;
	}
	
	public void initialize() throws MyException {
		try {
			MyLogger.log("MySqlConnConf : Initializing MySql Start");
			mysqlURL = AppConfig.get("mysql_host");
			mysqlPort = Integer.parseInt(AppConfig.get("mysql_port"));
            mysqlUser = AppConfig.get("mysql_user");
            mysqlPass = AppConfig.get("mysql_password");
            mysqlDatabase = AppConfig.get("mysql_database");
            
            MyLogger.log("MySqlConnConf : Initializing MySql End");
        	} catch (Exception e) {
			throw new MyException(e.toString());
		}
	}
	
	public static ResultSet executeSelectQuery(Connection sqlConnection, String selectQuery) throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        try{
            //query execution
            stmt = sqlConnection.createStatement();
            rs = stmt.executeQuery(selectQuery);
            return rs;
        }catch (Exception exp) {
            throw new Exception("executeSelectQuery : " + exp.toString() + " : " + selectQuery);
        }finally{
            System.gc();
        }	
    }
	
}
