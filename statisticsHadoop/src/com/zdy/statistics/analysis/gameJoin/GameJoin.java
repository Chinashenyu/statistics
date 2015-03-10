package com.zdy.statistics.analysis.gameJoin;

import java.sql.Connection;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;

public class GameJoin {

	private DB db;
	private Connection connection;
	
	public GameJoin() {
		db = MongoDBConnector.getDB();
		connection = MysqlConnect.getConnection();
	}
	
	public void analysisGameTable(){
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.game","true").append("message.table_id", "true"));
		group.put("initial", new BasicDBObject("count",0));
		group.put("$reduce", "function(doc,prev){prev.count += 1;}");
		group.put("condition", new BasicDBObject("message.type","join"));
		
		cmd.put("group", group);
		db.command(cmd);
		
		
	}
	
	
}
