package com.zdy.statistics.analysis.userInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

public class UserInfo {

	private Connection connection;
	private DB db;
	private DBCollection collection;
	
	public UserInfo() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	/**
	 * 更新基本信息
	 * //用户基本信息统计
		db.runCommand({"group":{
			"ns":"server",
			"key":{"message.user_id":true},
			"initial":{"sex":0,"nick_name":"","add_time":""},
			"$reduce":function(doc,prev){
				if(prev.add_time < doc.message.add_time){
					prev.add_time = doc.message.add_time;
					prev.sex = doc.message.sex;
					prev.nick_name = doc.message.sex;
				}
			},
			"condition":{"message.type":"base_info","message.result":1,"message.add_time":{"$gt":"2015-03-19 00:00:00","$lt":"2015-03-19 23:59:59"}}
		}})
	 * @return
	 */
	public String userBasicInfo(){
		
		String sql = "update user_info set sex = ? , nick_name = ? where user_id = ? ";
		PreparedStatement pstmt = null;
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.user_id","true"));
		group.put("initial", new BasicDBObject("sex",0).append("time", "").append("nick_name", ""));
		group.put("$reduce", "function(doc,prev){"+
					"if(prev.add_time < doc.message.add_time){"+
						"prev.count += doc.message.count;"+
						"prev.sex = doc.message.sex;"+
						"prev.nick_name = doc.message.sex;"+
					"}"+
				"}");
		
		String gtTime = DateTimeUtil.getyesterday()+" 00:00:00";
		String ltTime = DateTimeUtil.getyesterday()+" 23:59:59";
		group.put("condition", new BasicDBObject("message.type","base_info").append("message.add_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime)));
		cmd.put("group", group);
		
		CommandResult commandResult = db.command(cmd);
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			
			BasicBSONList retval = (BasicBSONList) commandResult.get("retval");
			for (Object object : retval) {
				DBObject dbObject = (DBObject) object;
				int count = (int)(double)dbObject.get("sex");
				String nickName = (String)dbObject.get("nickName");
				
				pstmt.setInt(1, count);
				pstmt.setString(2, nickName);
				
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
			"condition":{"$or":[{"message.consume":10600},{"message.obtain":10600}],"message.opera_time":{"$gt":"2015-03-19 00:00:00","$lt":"2015-03-19 23:59:59"}}
		}})
	 * @return
	 */
	public String analysisHuanle(int prop){
		
		String sql = "";
		if(prop == 10600){
			sql = "update user_info set huanledou = huanledou+? where user_id = ?";
		}else if(prop == 10500){
			sql = "update user_info set huanleka = huanleka+? where user_id = ?";
		}
		PreparedStatement pstmt = null;
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.user_id","true"));
		group.put("initial", new BasicDBObject("count",0));
		group.put("$reduce", "function(doc,prev){"+
					"if(doc.message.type == \"consume_prop\"){"+
						"prev.count -= doc.message.count;"+
					"}else if(doc.message.type == \"obtain_prop\"){"+
						"prev.count += doc.message.count;"+
					"}"+
				"}");
		BasicBSONList orBson = new BasicBSONList();
		orBson.add(new BasicDBObject("message.consume",prop));
		orBson.add(new BasicDBObject("message.obtain",prop));
		
		String gtTime = DateTimeUtil.getyesterday()+" 00:00:00";
		String ltTime = DateTimeUtil.getyesterday()+" 23:59:59";
		group.put("condition", new BasicDBObject("$or",orBson).append("message.opera_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime)));
		cmd.put("group", group);
		
		CommandResult commandResult = db.command(cmd);
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			
			BasicBSONList retval = (BasicBSONList) commandResult.get("retval");
			for (Object object : retval) {
				DBObject dbObject = (DBObject) object;
				int count = (int)(double)dbObject.get("count");
				int userId = (int)(double)dbObject.get("message.user_id");
				
				pstmt.setInt(1, count);
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
	 *  充值查询
	 *  db.runCommand({"group":{
			"ns":"server",
			"key":{"message.user_id":true},
			"initial":{"count":0,"times":0},
			"$reduce":function(doc,prev){
				prev.count += doc.message.count;
			},
			"condition":{"message.event":3,"message.opera_time":{"$gt":"2015-03-19 00:00:00","$lt":"2015-03-19 23:59:59"}}
		}})
	 * @return
	 */
	public String ananlysisRecharge(){
		
		String sql = "update user_info set recharge_count = recharge_count+? , recharge_times = recharge_times+? where user_id = ? ";
		PreparedStatement pstmt = null;
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.user_id","true"));
		group.put("initial", new BasicDBObject("count",0).append("times", 0));
		group.put("$reduce", "function(doc,prev){"+
					"prev.count += doc.message.count;"+
					"prev.times++;"+
				"}");
		
		String gtTime = DateTimeUtil.getyesterday()+" 00:00:00";
		String ltTime = DateTimeUtil.getyesterday()+" 23:59:59";
		group.put("condition", new BasicDBObject("message.event",3).append("message.opera_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime)));
		cmd.put("group", group);
		
		CommandResult commandResult = db.command(cmd);
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			
			BasicBSONList retval = (BasicBSONList) commandResult.get("retval");
			for (Object object : retval) {
				DBObject dbObject = (DBObject) object;
				int count = (int)(double)dbObject.get("count");
				int times = (int)(double)dbObject.get("times");
				int userId = (int)(double)dbObject.get("message.user_id");
				
				pstmt.setInt(1, count);
				pstmt.setInt(2, times);
				pstmt.setInt(3, userId);
				
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
	 * 游戏参与度查询、游戏胜率查询
	 * //斗地主参与局数查询
		db.runCommand({"group":{
			"ns":"server",
			"key":{"message.user_id":true},
			"initial":{"times":0},
			"$reduce":function(doc,prev){
				prev.times++;
			},
			"condition":{"message.game":1,"message.opera_time":{"$gt":"2015-03-19 00:00:00","$lt":"2015-03-19 23:59:59"}}
		}})
		
		@param game 1-斗地主 2-赢三张 3-斗牛
		@param type 1-游戏参与局数 2-游戏获胜次数
	 * @return 
	 */
	public String analysisGameJoin(int game,int type){
		String gameName = "";
		if(game == 1){
			if(type == 2){
				gameName = "doudizhu_winRate";
			}else{
				gameName = "doudizhu_join";
			}
		}else if(game == 2){
			if(type == 2){
				gameName = "ysz_winRate";
			}else{
				gameName = "ysz_join";
			}
		}else if(game == 3){
			if(type == 2){
				gameName = "douniu_winRate";
			}else{
				gameName = "douniu_join";
			}
		}
		String sql = "update user_info set "+gameName+" = "+gameName+"+? where user_id = ? ";
		PreparedStatement pstmt = null;
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.user_id","true"));
		group.put("initial", new BasicDBObject("times",0));
		group.put("$reduce", "function(doc,prev){"+
					"prev.times++;"+
				"}");
		
		String gtTime = DateTimeUtil.getyesterday()+" 00:00:00";
		String ltTime = DateTimeUtil.getyesterday()+" 23:59:59";
		BasicDBObject condition = new BasicDBObject("message.game",gameName).append("message.opera_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime));
		if(type == 2){
			condition.append("message.result", 1);
		}
		group.put("condition", condition);
		cmd.put("group", group);
		
		CommandResult commandResult = db.command(cmd);
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			
			BasicBSONList retval = (BasicBSONList) commandResult.get("retval");
			for (Object object : retval) {
				DBObject dbObject = (DBObject) object;
				int times = (int)(double)dbObject.get("times");
				int userId = (int)(double)dbObject.get("message.user_id");
				
				pstmt.setInt(1, times);
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
	
	
	public String analysisCompete(){
		return null;
	}
	
	public boolean isOnline(){
		return false;
	}
	
	public String lastLogin(){
		return null;
	}
	
	public static void main(String[] args) {
		new UserInfo().analysisLevel();
	}
}
