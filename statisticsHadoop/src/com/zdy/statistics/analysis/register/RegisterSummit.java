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
	 * @param isInsertUserInfo 如果为 true 则将新注册的用心 插入到用户信息表（user_info）中
	 * @return 注册数
	 */
	public int analysis(String startTime, String endTime ,boolean isInsertUserInfo){
		
		BasicDBObject query = new BasicDBObject();
		
		query.put("message.type", "registe");
		query.put("message.registe_time", new BasicDBObject("$gte",startTime).append("$lte", endTime));
		
		DBCursor cursor = collection.find(query);
		int count = collection.find(query).count();
		
		if(isInsertUserInfo){
			String sql = " insert into user_info (user_id,user_name) values (?,?)";
			PreparedStatement pstmt = null;
			
			try {
				connection.setAutoCommit(false);
				pstmt = connection.prepareStatement(sql);
				
				while(cursor.hasNext()){
					DBObject object = cursor.next();
					DBObject message = (DBObject) object.get("message");
					int userId = (int)message.get("user_id");
					String userName = (String) message.get("user_name");
					
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
		}
		
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
		
		int registerSummit = analysis(startTime,endTime,false);
		try {
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
	 */
	public void insertUserInfo(){
		
		Date now = new Date();
		String startTime = DateTimeUtil.secondCalculate(now, -5);
		String endTime = DateTimeUtil.secondCalculate(now, 0);
		
		analysis(startTime, endTime,true);
	}
	
	public static void main(String[] args) {
		new RegisterSummit().insertUserInfo();
	}
}
