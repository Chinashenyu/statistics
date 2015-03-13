package com.zdy.statistics.analysis.online;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mongodb.DB;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;

public class Online {

	private Connection connection;
	private DB db;
	
	public Online() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
	}
	
	public String analysis(){
		
		
		
		return null;
	}
	
}
