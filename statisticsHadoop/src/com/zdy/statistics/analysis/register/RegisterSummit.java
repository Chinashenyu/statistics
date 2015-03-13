package com.zdy.statistics.analysis.register;

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

public class RegisterSummit {

	private Connection connection;
	private DB db;
	private DBCollection collection;
	
	public RegisterSummit() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	public int analysis(String startTime, String endTime){
		
		BasicDBObject query = new BasicDBObject();
		
		query.put("message.type", "register");
		query.put("message.register_time", new BasicDBObject("$gte","").append("$lte", ""));
		
		int count = collection.find(query).count();
		
		return count;
	}
	
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
