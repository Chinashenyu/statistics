package com.zdy.statistics.analysis.common;

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
			username = properties.getProperty("mysql.username");
			password = properties.getProperty("mysql.password");
			
			System.out.println(username);
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(url, username, password);
			
			System.out.println("#####database connection success !#####");
		} catch (SQLException e) {
			System.out.println("#####DataBase connection exception !#####");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.out.println("#####DataBase driver not found exception !#####");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("#####DataBase configuration file(common.properties) read exception !#####");
			e.printStackTrace();
		}
		
	}
	
	public static Connection getConnection(){
		return connection;
	}
	
}
