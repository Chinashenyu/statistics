package com.zdy.statistics.analysis.huanle;

import java.sql.Connection;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;

public class AnalysisHuanLe {
	
	private Connection connection;
	private DB db;
	
	public AnalysisHuanLe() {
//		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
	}
	
	public String analysisConsume(){
		/*
		 * 分组查询
			db.runCommand({"group":{
				"ns":"server",
				"key":{"message.event":true},
				"initial":{"count":0},
				"$reduce":function(doc,prev){
					 prev.count += doc.message.count;
					},
				"condition":{"message.type":"consume_prop","message.consume":10600}
			}})
		 */
		BasicDBObject query = new BasicDBObject();
		//分组查询语句
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.event","true"));
		group.put("initial", new BasicDBObject("count",0));
		group.put("$reduce", "function(doc,prev){prev.count += doc.message.count;}");
		group.put("condition", new BasicDBObject("message.type","consume_prop").append("message.consume", 10600));
		
		query.put("group", group);
		
		
		CommandResult commandResult = db.command(query);
		
		BasicBSONList res = (BasicBSONList) commandResult.get("retval");
		
		for (Object object : res) {
			BasicDBObject dbObject = (BasicDBObject)object;
			System.out.println(dbObject.get("count"));
		}

		return null;
	}
	
	
	
	public static void main(String[] args) {
		new AnalysisHuanLe().analysisConsume();
	}
}
