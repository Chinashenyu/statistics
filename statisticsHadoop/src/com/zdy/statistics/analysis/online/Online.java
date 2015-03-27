package com.zdy.statistics.analysis.online;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class Online {

	private static Connection connection;
	private static DB db;
	
	private static Map<Integer,String> onlineMap = new HashMap<Integer,String>();
	
	public Online() {
		
	}
	
	static{
		db = MongoDBConnector.getDB();
		connection = MysqlConnect.getConnection();
		initAnalysis();
	}
	
	/**
	 * 查找 当前在线的用户
	 * //是否在线
		db.runCommand({"group":{
			"ns":"server",
			"key":{"message.user_id":true},
			"initial":{"time":"","isOnline":0},
			"$reduce":function(doc,prev){
				
				if(doc.message.type == "login"){
					if(prev.time < doc.message.login_time);{
						prev.time = doc.message.login_time;
						isOnline = 1;
					}
					
				}else if(doc.message.type == "logout"){
					if(prev.time < doc.message.logout_time);{
						prev.time = doc.message.logout_time;
						isOnline = 0;
					}
				}
			},
			"condition":{"$or":[{"message.type":"login"},{"message.type":"logout"}]},
			"finalize":function(prev){
				if(prev.isOnline == 0)
					return null;
			}
		}})
	 * @return
	 */
	public static void initAnalysis(){
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.user_id", "true"));
		group.put("initial", new BasicDBObject("time", "").append("isOnline", 0));
		group.put("$reduce", "function(doc,prev){"+
		
						"if(doc.message.type == 'login'){"+
							"if(prev.time < doc.message.login_time){"+
								"prev.time = doc.message.login_time;"+
								"prev.isOnline = 1;"+
							"}"+
							
						"}else if(doc.message.type == 'logout'){"+
							"if(prev.time < doc.message.logout_time){"+
								"prev.time = doc.message.logout_time;"+
								"prev.isOnline = 0;"+
							"}"+
						"}"+
					"}");
		BasicBSONList or = new BasicBSONList();
		or.add(new BasicDBObject("message.type", "login"));
		or.add(new BasicDBObject("message.type", "logout"));
		
		group.put("$or", or);
		group.put("finalize", "function(prev){"+
					"if(prev.isOnline == 0)"+
						"return null;"+
				"}");
		
		cmd.put("group", group);
		System.out.println("cmd :\n"+cmd);
		CommandResult commandResult = db.command(cmd);
		BasicBSONList retval = (BasicBSONList) commandResult.get("retval");
		for (Object object : retval) {
			if(object != null){
				DBObject dbObject = (DBObject)object;
				Integer userId = (int)(double)dbObject.get("message.user_id");
				String loginTime = (String)dbObject.get("message.time");
				onlineMap.put(userId, loginTime);
			}
		}
		
	}
	
	/**
	 * 定时查询登陆，退出数据
	 * 
	 */
	public void analysis(){
		
		BasicDBObject query = new BasicDBObject();
		
		BasicBSONList or = new BasicBSONList();
		or.add(new BasicDBObject("message.type","login"));
		or.add(new BasicDBObject("message.type","logout"));
		
		query.put("$or", or);
		
		Date now = new Date();
		String ltTime = DateTimeUtil.secondCalculate(now, 0);
		String gtTime = DateTimeUtil.secondCalculate(now, 5);
		
		BasicBSONList orTime = new BasicBSONList();
		orTime.add(new BasicDBObject("message.login_time", new BasicDBObject("$gt",gtTime).append("$lt", ltTime)));
		orTime.add(new BasicDBObject("message.logout_time", new BasicDBObject("$gt",gtTime).append("$lt", ltTime)));
		query.put("$or", orTime);
		
		BasicDBObject field = new BasicDBObject();
		field.put("message.type", 1);
		field.put("message.user_id", 1);
		field.put("message.login_time", 1);
		field.put("message.logout_time", 1);
		field.put("_id", 0);
		
		
		DBCollection collection = db.getCollection("server");
		DBCursor cursor = collection.find(query, field);
		
		while(cursor.hasNext()){
			System.out.println(cursor.next());
			BasicDBObject dbObject = (BasicDBObject) cursor.next();
			int userId = (int)dbObject.getDouble("message.user_id");
			String type = dbObject.getString("message.type");
			if(type != null && "login".equals(type)){
				String time = dbObject.getString("message.login_time");
				if(onlineMap.containsKey(userId)){
					if(time.compareTo(onlineMap.get(userId)) > 0)
						onlineMap.put(userId, time);
				}else{
					onlineMap.put(userId, time);
				}
			}else if(type != null && "logout".equals(type)){
				String time = dbObject.getString("message.logout_time");
				if(onlineMap.containsKey(userId)){
					if(time.compareTo(onlineMap.get(userId)) > 0)
						onlineMap.remove(userId);
				}else{
					
				}
			}
			
		}
		
		Integer[] userIds = new Integer[onlineMap.size()]; 
		userIds = onlineMap.keySet().toArray(userIds);
		System.out.println(Arrays.toString(userIds));
		String sql = " update user_info set is_online = 1 where user_id in "+Arrays.toString(userIds).replaceAll("\\[", "(").replaceAll("\\]", ")");
		System.out.println(sql);
		PreparedStatement pstmt = null;
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			
			pstmt.executeUpdate();
			connection.commit();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			
		}
		
	}
	public static void main(String[] args) {
		//Online.initAnalysis();
		new Online().analysis();
	}
}
