package com.zdy.statistics.analysis.dao;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

public class MyBasicDateSource {

	public static DataSource getDataSource() throws IOException{
		
		Properties prop = new Properties();
		prop.load(ClassLoader.getSystemResourceAsStream("common.properties"));
		String driverName = prop.getProperty("mysql.driverName");
		String url = prop.getProperty("mysql.url");
		String userName = prop.getProperty("mysql.username");
		String passWord = prop.getProperty("mysql.password");
		
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(driverName);
		ds.setUrl(url);
		ds.setUsername(userName);
		ds.setPassword(passWord);
		
		return ds;
	}
	
	public static void shutDownDataSource(DataSource ds) throws SQLException{
		BasicDataSource basicDS = (BasicDataSource)ds;
		basicDS.close();
	}
}
