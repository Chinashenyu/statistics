package com.zdy.statistics.analysis.top;

import java.sql.Connection;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;

public class Ranking {

	private Connection connection;
	private DB db;
	private DBCollection collection;
	
	public Ranking() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	/**
	 * 
	 * @return
	 */
	public String levelRankingAnalysis(){
		
		
		
		return null;
	}
	
}
