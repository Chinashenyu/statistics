package com.zdy.statistics.analysis.userInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class UserInfo {

	private Connection connection;
	private DB db;
	private DBCollection collection;
	
	public UserInfo() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	public String userBasicInfo(){
		return null;
	}
	
	/**
	 * 用户等级查询
	 * db.runCommand({"group":{
			"ns":"server",
			"key":{"message.user_id":true},
			"initial":{"counter":1,"time":"","level":0},
			"$reduce":function(doc,prev){
				if(prev.counter == 1){
					prev.time = doc.message.time;
					prev.level = doc.message.level;
				}else{
					if(prev.time < doc.message.time){
						prev.level = doc.message.level;
					}
				}
				prev.counter++;
			},
			"condition":{"message.type":"level"}
		}})
	 * @return
	 */
	public String analysisLevel(){
		
		String gtTime = DateTimeUtil.getyesterday()+" 00:00:00";
		String ltTime = DateTimeUtil.getyesterday()+" 23:59:59";
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("message.type", "level");
		dbObject.put("message.time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime));
		DBCursor cursor = collection.find(dbObject);
		
		String sql = "update user_info set level = ? where user_id = ?";
		PreparedStatement pstmt = null;
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			
			while(cursor.hasNext()){
				DBObject userLevel = cursor.next();
				int userId = (int)(double)userLevel.get("message.user_id");
				int level = (int)(double)userLevel.get("message.level");
				pstmt.setInt(1, level);
				pstmt.setInt(2, userId);
				
				pstmt.addBatch();
			}
			
			pstmt.executeBatch();
			connection.commit();
			
		} catch (SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}finally{
			if(pstmt != null){try { pstmt.close(); } catch (SQLException e) { }}
			if(connection != null){try { connection.close(); } catch (SQLException e) { }}
		}
		
		
		
		return null;
	}
	
	/**
	 * 用户欢乐豆、卡总计统计
	 *  db.runCommand({"group":{
			"ns":"server",
			"key":{"message.user_id":true},
			"initial":{"type":"","count":0},
			"$reduce":function(doc,prev){
				if(doc.message.type == "consume_prop"){
					prev.count -= doc.message.count;
				}else if(doc.message.type == "obtain_prop"){
					prev.count += doc.message.count;
				}
			},
			"condition":{"$or":[{"message.type":"consume_prop"},{"message.type":"obtain_prop"}]}
		}})
	 * @return
	 */
	public String analysisHuanle(){
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.user_id","true"));
		group.put("initial", new BasicDBObject("count",0));
		
		
		cmd.put("group", group);
		db.command(cmd);
		return null;
	}
	
	public String ananlysisRecharge(){
		return null;
	}
	
	public String analysisGameJoin(){
		return null;
	}
	
	public String analysisCompete(){
		return null;
	}
	
	public boolean isOnline(){
		return false;
	}
	
	public String lastLogin(){
		return null;
	}
}
