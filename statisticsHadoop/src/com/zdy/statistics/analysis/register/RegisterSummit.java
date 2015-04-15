package com.zdy.statistics.analysis.register;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

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

public class RegisterSummit {

	private DB db;
	private DBCollection collection;
	
	/**
	 * 
	 */
	public RegisterSummit() {
		
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	/**
	 * 分析新注册的用户数，同时在user_info表中新增，新注册用户的信息；
	 * 
	 * @param startTime
	 * @param endTime
	 * @param isInsertUserInfo 如果为 true 则将新注册的用心 插入到用户信息表（user_info）中
	 * @return 注册数
	 */
	public int analysis(String startTime, String endTime ,boolean isInsertUserInfo){
		
		BasicDBObject query = new BasicDBObject();
		
		query.put("message.type", "registe");
		query.put("message.registe_time", new BasicDBObject("$gte",startTime).append("$lte", endTime));
		
		DBCursor cursor = collection.find(query);
		int count = cursor.count();
		
		return count;
	}
	
	/**
	 * 将注册用户数 添加到 register_summit（注册峰值表） 中
	 */
	public void insertResult(){
		
		Connection connection = null;
		
		String sql = " insert into register_summit (count,start_time,end_time) values (?,?,?) ";
		PreparedStatement pstmt = null;
		
		Date date = new Date();
		String startTime = DateTimeUtil.hourCalculate(date, -1)+":00:00";
		String endTime = DateTimeUtil.hourCalculate(date, -1)+":59:59";
		
		int registerSummit = analysis(startTime,endTime,false);
		try {
			connection = MysqlConnect.getConnection();
			connection.setAutoCommit(false);
			
			pstmt = connection.prepareStatement(sql);
			pstmt.setInt(1, registerSummit);
			pstmt.setString(2, startTime);
			pstmt.setString(3, endTime);
			
			pstmt.executeUpdate();
			connection.commit();
		} catch (SQLException e) {
			if(connection != null){try { connection.rollback(); } catch (SQLException se) { }}
			e.printStackTrace();
		}finally{
			if(pstmt != null){try { pstmt.close(); } catch (SQLException e) { }}
			if(connection != null){try { connection.close(); } catch (SQLException e) { }}
		}
	}
	
	/**
	 * 添加注册用户信息
	 * //新增用户信息
		db.runCommand({"group":{
			"ns":"server",
			"key":{"message.user_id":true},
			"initial":{"user_id":0,"user_name":"","nick_name":""},
			"$reduce":function(doc,prev){
				if(doc.message.type == "registe"){
					prev.user_id = doc.message.user_id;
					prev.user_name = doc.message.user_name;
					
				}else{
					prev.nick_name = doc.message.nick_name;
				}
			},
			"condition":{"$or":[{"message.type":"registe","message.registe_time":{"$gte":"2015-04-14 00:00:00","$lte":"2015-04-14 23:00:00"}},{"message.type":"base_info","message.add_time":{"$gte":"2015-04-14 00:00:00","$lte":"2015-04-14 23:00:00"}}]}
		}})
	 */
	public void insertUserInfo(){
		
		Date now = new Date();
		String startTime = DateTimeUtil.secondCalculate(now, -5);
		String endTime = DateTimeUtil.secondCalculate(now, 0);
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.user_id","true"));
		group.put("initial", new BasicDBObject("user_id",0).append("user_name", "").append("nick_name", ""));
		group.put("$reduce", "function(doc,prev){"+
					"if(doc.message.type == \"registe\"){"+
						"prev.user_id = doc.message.user_id;"+
						"prev.user_name = doc.message.user_name;"+
					"}else if(doc.message.type == \"base_info\"){"+
						"prev.nick_name = doc.message.nick_name;"+
					"}"+
				"}");
		BasicBSONList orBson = new BasicBSONList();
		orBson.add(new BasicDBObject("message.type","registe").append("message.registe_time", new BasicDBObject("$gte",startTime).append("$lte", endTime)));
		orBson.add(new BasicDBObject("message.type","base_info").append("message.add_time", new BasicDBObject("$gte",startTime).append("$lte", endTime)));
		
		group.put("condition", new BasicDBObject("$or",orBson));
		cmd.put("group", group);
		
		CommandResult commandResult = db.command(cmd);
		
		BasicBSONList retval = (BasicBSONList) commandResult.get("retval");
		
		String sql = " insert into user_info (user_id,user_name,nick_name) values (?,?,?)";
		Connection connection = null;
		PreparedStatement pstmt = null;
		
		try {
			connection = MysqlConnect.getConnection();
			connection.setAutoCommit(false);
			
			pstmt = connection.prepareStatement(sql);
			
			for(Object object : retval){
				DBObject dbObject = (DBObject) object;
				int userId = (int)(double)dbObject.get("user_id");
				String userName = (String) dbObject.get("user_name");
				if(userId == 0 && "".equals(userName)){
					userId = (int)(double)dbObject.get("message.user_id");
					userName = "机器人";
				}
				String nickName = (String) dbObject.get("nick_name");
				
				pstmt.setInt(1, userId);
				pstmt.setString(2, userName);
				pstmt.setString(3, nickName);
				System.out.println(pstmt);
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
		
	}
	
	public static void main(String[] args) {
		new RegisterSummit().insertUserInfo();
	}
}
