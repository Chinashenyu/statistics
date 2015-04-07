package com.zdy.statistics.analysis.online;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
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

public class OnlineAnalysis {

	private static Connection connection;
	private static DB db;
	
	private static Map<Integer,String> onlineMap = new HashMap<Integer,String>();
	private static Map<Integer,String> onlineCountMap = new HashMap<Integer,String>();
	
	private static Logger logger = Logger.getLogger(OnlineAnalysis.class);
	
	public OnlineAnalysis() {
		
	}
	
	static{
		db = MongoDBConnector.getDB();
		connection = MysqlConnect.getConnection();
		initAnalysis();
		logger.info("OnlineAnalysis 静态块 执行");
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
						prev.isOnline = 1;
					}
					
				}else if(doc.message.type == "logout"){
					if(prev.time < doc.message.logout_time);{
						prev.time = doc.message.logout_time;
						prev.isOnline = 0;
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
		logger.info("OnlineAnalysis 初始化onlineMap方法  执行");
		if(onlineMap.size() > 0){
			onlineMap.clear();
		}
		
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
//		System.out.println("cmd :\n"+cmd);
		CommandResult commandResult = db.command(cmd);
		BasicBSONList retval = (BasicBSONList) commandResult.get("retval");
		for (Object object : retval) {
			if(object != null){
				DBObject dbObject = (DBObject)object;
				Integer userId = (int)(double)dbObject.get("message.user_id");
				String loginTime = (String)dbObject.get("time");
				onlineMap.put(userId, loginTime);
			}
		}
		
//		System.out.println(onlineMap.size());
	}
	
	/**
	 * 定时器 调度此方法
	 */
	public void analysis(){
		logger.info("OnlineAnalysis 当前在线 方法  执行");
		BasicDBObject query = new BasicDBObject();
		
//		BasicBSONList or = new BasicBSONList();
//		or.add(new BasicDBObject("message.type","login"));
//		or.add(new BasicDBObject("message.type","logout"));
//		
//		query.put("$or", or);
		
		Date now = new Date();
		String gtTime = DateTimeUtil.secondCalculate(now, -5);
		String ltTime = DateTimeUtil.secondCalculate(now, 0);
		
		BasicBSONList or = new BasicBSONList();
		or.add(new BasicDBObject("message.login_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime)).append("message.type","login"));
		or.add(new BasicDBObject("message.logout_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime)).append("message.type","logout"));
		query.put("$or", or);
		
		BasicDBObject field = new BasicDBObject();
		field.put("message.type", 1);
		field.put("message.user_id", 1);
		field.put("message.login_time", 1);
		field.put("message.logout_time", 1);
		field.put("_id", 0);
//		System.out.println(query);
		
		DBCollection collection = db.getCollection("server");
		DBCursor cursor = collection.find(query, field);
		logger.info("cursor count :"+cursor.count());
		while(cursor.hasNext()){
			
			BasicDBObject dbObject = (BasicDBObject) cursor.next();
//			System.out.println(dbObject);
			BasicDBObject message = (BasicDBObject) dbObject.get("message");
			int userId = (int)message.getDouble("user_id");
//			System.out.println("user_id"+userId);
			String type = message.getString("type");
			if(type != null && "login".equals(type)){
				logger.info("onlinMap 添加 用户");
				String time = message.getString("login_time");
				if(onlineMap.containsKey(userId)){
					if(time.compareTo(onlineMap.get(userId)) > 0)
						onlineMap.put(userId, time);
				}else{
					onlineMap.put(userId, time);
				}
			}else if(type != null && "logout".equals(type)){
				logger.info("onlinMap 删除 用户");
				String time = message.getString("logout_time");
				if(onlineMap.containsKey(userId)){
					if(time.compareTo(onlineMap.get(userId)) > 0)
						onlineMap.remove(userId);
				}else{
					
				}
			}
			
		}
//		System.out.println(cursor.count());
//		System.out.println(onlineMap.size());
		onlineCountMap.putAll(onlineMap);
		
	}
	
	/**
	 * RMI 服务 调用此方法；
	 * @return Map<Integer,String> onlineMap;
	 */
	public static Map<Integer,String> getOnlineMap(){
		return onlineMap;
	}
	
	/**
	 * 在线峰值 定时器 调度
	 * @param args
	 */
	public void analysisOnlineCount(){
		logger.info("OnlineAnalysis 在线峰值方法  执行");
		int count = onlineCountMap.size();
		onlineCountMap.clear();
		
		String sql = " insert into online_count (online_count,time) values (?,?)";
		
		PreparedStatement pstmt = null;
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			pstmt.setInt(1, count);
			pstmt.setDate(2, new java.sql.Date(new Date().getTime()));
			pstmt.setString(2, DateTimeUtil.minuteCalculate(new Date(), 0));
			
			pstmt.executeUpdate();
			connection.commit();
			
		}  catch (SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		//Online.initAnalysis();
		new OnlineAnalysis().analysis();
		
//		initAnalysis();
	}
}
