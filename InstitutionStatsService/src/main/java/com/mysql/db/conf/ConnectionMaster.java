package com.mysql.db.conf;

import java.sql.Connection;
import java.sql.DriverManager;

import com.mps.utils.MyException;
import com.mps.utils.MyLogger;

public class ConnectionMaster {

	//method to get MysqlConnection
	public static Connection getMySqlConnection(MySqlConnConf conf) throws MyException{
		Connection conn = null;
		try {
			Class.forName(conf.getDriver());
			conn = DriverManager.getConnection("jdbc:mysql://" + conf.getIpHost() + ":" + conf.getPort() + "/" + conf.getDatabase(), conf.getUser(), conf.getPassword());
			return conn;
		} catch (Exception e) {
			MyLogger.error("ConnectionMaster: getMySqlConnection: EXCEPTION: " + e);
		}
		return conn;
	}
}
