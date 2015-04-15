package com.zdy.statistics.analysis.recharge;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class AnalysisRecharge {

	private DB db;
	
	public AnalysisRecharge() {
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
        	BasicDBObject dbbject = (BasicDBObject) cur.next();
        	BasicDBObject message = (BasicDBObject) dbbject.get("message");
        	incomeOfDay += ((int)message.get("count"));
        }
        
        int[] dayAnalysis = new int[2];
        dayAnalysis[0] = rechargeUsersOfDay;
        dayAnalysis[1] = incomeOfDay;
        
		return dayAnalysis;
	}
	
	public int[] findYesterdayRecharge(){
		Connection connection = null;
		
		int totalRecahrgerUsers = 0;
		int totalIncome = 0;
		
		String date = DateTimeUtil.dateCalculate(new Date(), -2);
		String sql = " select * from recharge_info where date =  '"+date+"'";
		System.out.println(sql);
		PreparedStatement pstmt = null;
		
		try {
			connection = MysqlConnect.getConnection();
			pstmt = connection.prepareStatement(sql);
			ResultSet resultset = pstmt.executeQuery();
			while(resultset.next()){
				totalRecahrgerUsers = resultset.getInt("total_recharge_users");
				totalIncome = resultset.getInt("total_income");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(pstmt != null){try { pstmt.close(); } catch (SQLException e) { }}
			if(connection != null){try { connection.close(); } catch (SQLException e) { }}
		}
		
		int[] total = new int[2];
		total[0] = totalRecahrgerUsers;
		total[1] = totalIncome;
		
		System.out.println(Arrays.toString(total));
		
		return total;
	}
	
	public int queryRegister(){
		
		DBCollection collection = db.getCollection("server");
		
		
		BasicDBObject query = new BasicDBObject();
		query.put("message.type", "registe");
        
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
		
		double payRate = 0d;
		if(registerCount != 0){
			payRate = ((double)totalRecahrgerUsers/(double)registerCount)*100;
		}
		double registerARPU = 0d;
		if(registerCount != 0){
			registerARPU = (double)totalIncome/(double)registerCount;
		}
		double payARPU = 0d;
		if(totalRecahrgerUsers != 0){
			payARPU = (double)totalIncome/(double)totalRecahrgerUsers;
		}
		
		DecimalFormat df = new DecimalFormat("#.00");
		//
		result[0] = totalRecahrgerUsers+"";
		result[1] = dayAnalysis[0]+"";
		result[2] = totalIncome+"";
		result[3] = dayAnalysis[1]+"";
		result[4] = df.format(payRate);
		result[5] = df.format(registerARPU);
		result[6] = df.format(payARPU);
		
		System.out.println(Arrays.toString(result));
		
		return result;
	}
	
	public void insertResult(){
		
		String[] result = gether();
		
		if(result != null && result.length == 7){
			Connection connection = null;
			PreparedStatement pstmt = null;
			
			String sql = " insert into recharge_info (total_recharge_users,day_recharge_users,total_income,day_income,pay_rate,register_arpu,pay_arpu,date) values (?,?,?,?,?,?,?,?)";
			
			try {
				connection = MysqlConnect.getConnection();
				connection.setAutoCommit(false);
				
				pstmt = connection.prepareStatement(sql);
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
					pstmt.close();
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
