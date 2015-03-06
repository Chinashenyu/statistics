package com.zdy.statistics.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MysqlConnect {
	
	public static Connection connection;
	
	static {
		
		String url = "";
		String username = "";
		String password = "";
		
		try {
			ClassLoader classLoader = ClassLoader.getSystemClassLoader();
			
			Properties properties = new Properties();
			properties.load(classLoader.getResourceAsStream("common.properties"));
			url = properties.getProperty("mysql.url");
			username = properties.getProperty("username");
			password = properties.getProperty("password");
			
			Class.forName("com.mysql.jdbc.driver");
			connection = DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			System.out.println("");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("");
			e.printStackTrace();
		}
		
	}
	
	public static Connection getConnection(){
		return connection;
	}
	
}
