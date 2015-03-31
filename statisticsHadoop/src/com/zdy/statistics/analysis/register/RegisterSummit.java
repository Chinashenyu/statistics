package com.zdy.statistics.analysis.register;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class RegisterSummit {

	private Connection connection;
	private DB db;
	private DBCollection collection;
	
	/**
	 * 
	 */
	public RegisterSummit() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	/**
	 * 分析新注册的用户数，同时在user_info表中新增，新注册用户的信息；
	 * 
	 * @param startTime
	 * @param endTime
	 * @return 注册数
	 */
	public int analysis(String startTime, String endTime){
		
		String sql = " insert into user_info (user_id,user_name) values (?,?)";
		PreparedStatement pstmt = null;
		
		BasicDBObject query = new BasicDBObject();
		
		query.put("message.type", "register");
		query.put("message.register_time", new BasicDBObject("$gte",startTime).append("$lte", endTime));
		
		DBCursor cursor = collection.find(query);
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			
			while(cursor.hasNext()){
				DBObject object = cursor.next();
				int userId = (int)(double)object.get("message.user_id");
				String userName = (String) object.get("message.user_name");
				
				pstmt.setInt(1, userId);
				pstmt.setString(2, userName);
				
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
		
		
		int count = collection.find(query).count();
		
		
		return count;
	}
	
	/**
	 * 将注册用户数 添加到 register_summit（注册峰值表） 中
	 */
	public void insertResult(){
		String sql = " insert into register_summit (count,start_time,end_time) values (?,?,?) ";
		PreparedStatement pstmt = null;
		
		Date date = new Date();
		String startTime = DateTimeUtil.hourCalculate(date, -1)+":00:00";
		String endTime = DateTimeUtil.hourCalculate(date, -1)+":59:59";
		
		int registerSummit = analysis(startTime,endTime);
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			pstmt.setInt(1, registerSummit);
			pstmt.setString(2, startTime);
			pstmt.setString(3, endTime);
			
			pstmt.executeUpdate();
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
		new RegisterSummit().insertResult();
	}
}
