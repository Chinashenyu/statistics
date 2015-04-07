package com.zdy.statistics.analysis.firstConsume;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.analysis.contrastCache.EventContrast;
import com.zdy.statistics.util.DateTimeUtil;

public class FirstConsume {

	private Connection connection;
	private DB db;
	
	public FirstConsume(){
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
	}
	
	/**
	 * db.runCommand({"group":{
		"ns":"server",
		"key":{"message.user_id":true},
		"initial":{"time":"","event":0,"count":1},
		"$reduce":function(doc,prev){
			if(prev.count == 1){
				prev.time = doc.message.opera_time;
				prev.event = doc.message.event;
			}else{
				if(prev.time > doc.message.opera_time){
					 prev.event = doc.message.event;
					 prev.time = doc.message.opera_time;
				}
			}
			prev.count++;
			},
		"condition":{"message.type":"consume_prop"}
		}})
		
		//充值后首次消费
		db.runCommand({"group":{
		"ns":"server",
		"key":{"message.user_id":true},
		"initial":{"time":"","event":0,"count":1,"first":""},
		"$reduce":function(doc,prev){
			if(prev.count == 1){
				prev.time = doc.message.opera_time;
				prev.event = doc.message.event;
			}else{
				if(prev.event == 3){
					prev.first += doc.message.event+'@';
				}
			}
			
			prev.count++;
			},
		"condition":{"message.type":"consume_prop"}
		}})
	 */
	public String analysis(int type){
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.user_id","true"));
		if(type == 1){
			group.put("initial", new BasicDBObject("time","").append("event", 0).append("count", 1));
			group.put("$reduce", "function(doc,prev){"
					+ "if(prev.count == 1){"
						+ "prev.time = doc.message.opera_time;"
						+ "prev.event = doc.message.event;"
						+ "}else{"
							+ "if(prev.time > doc.message.opera_time){ "
									+ "prev.event = doc.message.event; "
									+ "prev.time = doc.message.opera_time;"
							+ "}"
						+ "}"
						+ "prev.count++;"
					+ "}");
		}else if(type == 2){
			group.put("initial", new BasicDBObject("time","").append("event", 0).append("count", 1).append("first", ""));
			group.put("$reduce", "function(doc,prev){"
					+ "if(prev.count == 1){"
						+ "prev.time = doc.message.opera_time;"
						+ "prev.event = doc.message.event;"
						+ "}else{"
							+ "if(prev.event == 3){ "
									+ "prev.first += doc.message.event+'@'; "
							+ "}"
						+ "}"
						+ "prev.count++;"
					+ "}");
		}
		
		group.put("condition", new BasicDBObject("message.type","consume_prop"));
		
		cmd.put("group", group);
		CommandResult commandResult = db.command(cmd);
		
		Map<Integer,Integer> resMap = new HashMap<Integer, Integer>();
		Map<Integer, String> eventMap = EventContrast.getEventMap();
		
		BasicBSONList retval = (BasicBSONList)commandResult.get("retval");
		for (Object object : retval) {
			BasicDBObject dbObject = (BasicDBObject)object;
			if(type == 1){
				Integer eventId = (int)((double)dbObject.get("event"));
//				Integer count = dbObject.getInt("count");
				if(resMap.containsKey(eventId)){
					resMap.put(eventId, resMap.get(eventId)+1);
				}else{
					resMap.put(eventId, 1);
				}
				
				
			}else if(type == 2){
				String[] eventIdsStr = dbObject.getString("first").split("@");
				for (String idStr : eventIdsStr) {
					if(idStr != null && !"".equals(idStr)){
						Integer eventId = Integer.parseInt(idStr);
						if(resMap.containsKey(eventId)){
							resMap.put(eventId, resMap.get(eventId)+1);
						}else{
							resMap.put(eventId, 1);
						}
					}
				}
			}
			
		}
		
		Map<String,Integer> jsonMap = new HashMap<String, Integer>();
		for(Entry<Integer, Integer> entry : resMap.entrySet()){
			Integer key = entry.getKey();
			Integer value = entry.getValue();
			
			if(eventMap.containsKey(key)){
				jsonMap.put(eventMap.get(key), value);
			}else{
				EventContrast.updateEventMap();
				if(eventMap.containsKey(key)){
					jsonMap.put(eventMap.get(key), value);
				}else{
					jsonMap.put(key+"", value);
				}
			}
			
		}
		
		
		return JSONObject.fromObject(jsonMap).toString();
	}
	
	public void insertResult(){
		String sql = " insert into first_consume (result_set,type,date) values(?,?,?)";
		PreparedStatement pstmt = null;
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			for(int i=1;i<=2;i++){
				String resJson = analysis(i);
				pstmt.setString(1, resJson);
				pstmt.setInt(2, i);
				pstmt.setString(3, DateTimeUtil.dateCalculate(new Date(), -1));
				
				pstmt.addBatch();
			}
			
			pstmt.executeBatch();
			connection.commit();
		} catch (SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		}finally{
			if(pstmt != null){try { pstmt.close(); } catch (SQLException e) { }}
			if(connection != null){try { connection.close(); } catch (SQLException e) { }}
		}
		
	}
	
	public static void main(String[] args) {
		new FirstConsume().insertResult();
	}
}
