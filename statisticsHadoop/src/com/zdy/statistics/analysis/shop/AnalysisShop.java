/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zdy.statistics.analysis.shop;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

/**
 *
 * @author Administrator
 */
public class AnalysisShop {
  
	private Connection connection;
	
	public AnalysisShop() {
		connection = MysqlConnect.getConnection();
	}
	
    public int shopSell() throws UnknownHostException{
        
        DB db = MongoDBConnector.getDB();
        
        DBCollection collection = db.getCollection("server");
        
        BasicDBObject query = new BasicDBObject();
        query.put("message.type", "obtain_prop");
        query.put("message.event", 1);
        java.util.Date now = new java.util.Date();
        String gtTime = DateTimeUtil.dateCalculate(now, -1) + " 00:00:00";
        String ltTime = DateTimeUtil.dateCalculate(now, -1) + " 23:59:59";
        query.put("message.opera_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime));
        DBCursor cur = collection.find(query);
        int count = 0;
        while(cur.hasNext()){
        	DBObject dbObject = cur.next();
        	count += ((int)dbObject.get("message.count"));
        }
        
        return count;
    }
    
    public void insertResult(int shopSellCount){
    	String sql = "insert into shop_sell (count,date) values (?,?)";
    	
    	try {
    		connection.setAutoCommit(false);
			PreparedStatement pstmt = connection.prepareStatement(sql);
			pstmt.setInt(1, shopSellCount);
			pstmt.setString(2, DateTimeUtil.dateCalculate(new Date(), -1));
			
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
    
    public void execShopAnalysis(){
    	try {
			int count = shopSell();
			insertResult(count);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
    }
    
    public static void main(String[] args) {
		new AnalysisShop().execShopAnalysis();
	}
}
