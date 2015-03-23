package com.zdy.statistics.analysis.common;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import com.zdy.statistics.analysis.dao.MyBasicDateSource;

public class MysqlConnect {
	
	public static Connection connection;
	public static DataSource dataSource;
	
	static {
		
		try {
			dataSource = MyBasicDateSource.getDataSource();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static Connection getConnection(){
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
}
