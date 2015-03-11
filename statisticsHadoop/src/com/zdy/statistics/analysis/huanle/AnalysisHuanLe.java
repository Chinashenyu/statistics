package com.zdy.statistics.analysis.huanle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.analysis.contrastCache.EventContrast;

public class AnalysisHuanLe {
	
	private Connection connection;
	private DB db;
	
	public AnalysisHuanLe() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
	}
	
	/*
	 * type:1-欢乐豆消耗，2-欢乐豆回收，3-欢乐卡消耗，4-欢乐卡回收
	 */
	public String analysisConsume(int type){
		/*
		 * 分组查询
		 *db.runCommand({"group":{
		 *	"ns":"server",
		 *	"key":{"message.event":true},
		 *	"initial":{"count":0},
		 *	"$reduce":function(doc,prev){
		 *		 prev.count += doc.message.count;
		 *		},
		 *	"condition":{"message.type":"consume_prop","message.consume":10600}
		 *}})
		 */
		BasicDBObject query = new BasicDBObject();
		//分组查询语句
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.event","true"));
		group.put("initial", new BasicDBObject("count",0));
		group.put("$reduce", "function(doc,prev){prev.count += doc.message.count;}");
		switch (type) {
		case 1:
			group.put("condition", new BasicDBObject("message.type","consume_prop").append("message.consume", 10600));
			break;
		case 2:
			group.put("condition", new BasicDBObject("message.type","obtain_prop").append("message.obtain", 10600));
			break;
		case 3:
			group.put("condition", new BasicDBObject("message.type","consume_prop").append("message.consume", 10500));
			break;
		case 4:
			group.put("condition", new BasicDBObject("message.type","obtain_prop").append("message.obtain", 10500));
			break;

		default:
			break;
		}
		
		query.put("group", group);
		
		
		CommandResult commandResult = db.command(query);
		
		BasicBSONList res = (BasicBSONList) commandResult.get("retval");
		
		Map<Integer,String> eventMap = EventContrast.getEventMap();
		
		Map<String,Double> resMap = new HashMap<String,Double>();
		for (Object object : res) {
			BasicDBObject dbObject = (BasicDBObject)object;
			Double eventIdDoble = (Double)dbObject.get("message.event");
			double d = eventIdDoble;
			Integer eventId = (int)d;
			Double count = (Double)dbObject.get("count");
			String eventName = "";
			if(eventMap.containsKey(eventId)){
				eventName = eventMap.get(eventId);
			}else{
				EventContrast.updateEventMap();
				if(eventMap.containsKey(eventId)){
					eventName = eventMap.get(eventId);
				}else{
					eventName = "the event_id is not exists !";
				}
			}
			
			resMap.put(eventName, count);
		}

		return JSONObject.fromObject(resMap).toString();
	}
	
	public void insertReult(){
		
		String sql = " insert into huanle (result_set,type,date) values (?,?,?)";
		PreparedStatement pstmt = null;
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			for(int i=1;i<=4;i++){
				String resultSet = analysisConsume(i);
				pstmt.setString(1, resultSet);
				pstmt.setInt(2, i);
				pstmt.setDate(3, new java.sql.Date(new Date().getTime()));
				pstmt.addBatch();
			}
			
			pstmt.executeBatch();
			connection.commit();
		} catch (SQLException e) {
			try {
				System.out.println("-------------------");
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}finally{
			try {
				pstmt.close();
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
//		String json = new AnalysisHuanLe().analysisConsume();
//		System.out.println(json);
		
		new AnalysisHuanLe().insertReult();
	}
}
