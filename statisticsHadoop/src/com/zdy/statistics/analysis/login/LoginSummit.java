package com.zdy.statistics.analysis.login;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class LoginSummit {

	private Connection connection;
	private DB db;
	private DBCollection collection;
	private String gtTime;
	private String ltTime;
	
	public LoginSummit() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	public int analysis(){
		
		BasicDBObject query = new BasicDBObject();
		query.put("message.type", "login");
		
		query.put("message.login_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime));
		
		int count = collection.find(query).count();
		
		return count;
	}
	
	public void insertResult(){
		String sql = "insert into login_summit (count,start_time,end_time) values (?,?,?)";
		PreparedStatement pstmt = null;
		
		Date date = new Date();
		gtTime = DateTimeUtil.minuteCalculate(date, -15);
		ltTime = DateTimeUtil.minuteCalculate(date, 0);
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			pstmt.setInt(1, analysis());
			pstmt.setString(2, gtTime);
			pstmt.setString(3, ltTime);
			
			pstmt.executeUpdate();
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
		new LoginSummit().insertResult();
	}
}
