package com.zdy.statistics.analysis.recharge;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class AnalysisRecharge {

	private Connection connection;
	private DB db;
	
	public AnalysisRecharge() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
	}
	
	public int[] analysis(){
		
		DBCollection collection = db.getCollection("server");
		
		
		BasicDBObject query = new BasicDBObject();
		query.put("message.type", "consume_prop");
		query.put("message.event", 3);
		java.util.Date now = new java.util.Date();
		String gtTime = DateTimeUtil.dateCalculate(now, -1) + " 00:00:00";
        String ltTime = DateTimeUtil.dateCalculate(now, -1) + " 23:59:59";
        query.put("message.opera_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime));
        
        DBCursor cur = collection.find(query);
        
        
        //日付费用户数
        int rechargeUsersOfDay = cur.count();
        
        //日收入
        int incomeOfDay = 0;
        while(cur.hasNext()){
        	incomeOfDay += ((int)cur.next().get("message.count"));
        }
        
        int[] dayAnalysis = new int[2];
        dayAnalysis[0] = rechargeUsersOfDay;
        dayAnalysis[1] = incomeOfDay;
        
		return dayAnalysis;
	}
	
	public int[] findYesterdayRecharge(){
		
		int totalRecahrgerUsers = 0;
		int totalIncome = 0;
		
		String yesterday = DateTimeUtil.getyesterday();
		String sql = " select * from recharge_info where date =  "+yesterday;
		
		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);
			ResultSet resultset = pstmt.executeQuery();
			while(resultset.next()){
				totalRecahrgerUsers = resultset.getInt("total_recharge_users");
				totalIncome = resultset.getInt("total_income");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int[] total = new int[2];
		total[0] = totalRecahrgerUsers;
		total[1] = totalIncome;
		
		return total;
	}
	
	public int queryRegister(){
		
		DBCollection collection = db.getCollection("server");
		
		
		BasicDBObject query = new BasicDBObject();
		query.put("message.type", "register");
        
        DBCursor cur = collection.find(query);
		
		return cur.count();
	}
	
	public String[] gether(){
		
		String[] result = new String[7];
		
		int[] dayAnalysis = analysis();
		int[] totalAnalysis = findYesterdayRecharge();
		int registerCount = queryRegister();
		
		//result set
		int totalRecahrgerUsers = dayAnalysis[0] + totalAnalysis[0];
		int totalIncome = dayAnalysis[1] + totalAnalysis[1];
		
		double payRate = 0;
		if(registerCount != 0){
			payRate = (totalRecahrgerUsers/registerCount)*100;
		}
		double registerARPU = 0;
		if(registerCount != 0){
			registerARPU = totalIncome/registerCount;
		}
		double payARPU = 0;
		if(totalRecahrgerUsers != 0){
			payARPU = totalIncome/totalRecahrgerUsers;
		}
		
		//
		result[0] = totalRecahrgerUsers+"";
		result[1] = dayAnalysis[0]+"";
		result[2] = totalIncome+"";
		result[3] = dayAnalysis[1]+"";
		result[4] = payRate+"";
		result[5] = registerARPU+"";
		result[6] = payARPU+"";
		
		return result;
	}
	
	public void insertResult(){
		
		String[] result = gether();
		
		if(result != null && result.length == 7){
			String sql = " insert into recharge_info (total_recharge_users,day_recharge_users,total_income,day_income,pay_rate,register_arpu,pay_arpu,date) values (?,?,?,?,?,?,?,?)";
			
			try {
				connection.setAutoCommit(false);
				PreparedStatement pstmt = connection.prepareStatement(sql);
				pstmt.setInt(1, Integer.parseInt(result[0]));
				pstmt.setInt(2, Integer.parseInt(result[1]));
				pstmt.setInt(3, Integer.parseInt(result[2]));
				pstmt.setInt(4, Integer.parseInt(result[3]));
				pstmt.setDouble(5, Double.parseDouble(result[4]));
				pstmt.setDouble(6, Double.parseDouble(result[5]));
				pstmt.setDouble(7, Double.parseDouble(result[6]));
				pstmt.setString(8, DateTimeUtil.dateCalculate(new Date(), -1));
				
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
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}
				
	}
	
	public static void main(String[] args) {
		new AnalysisRecharge().insertResult();
	}
	
}
