package com.zdy.statistics.analysis.login;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class Login {

	private Connection connection;
	private DB db;
	private DBCollection collection;
	private String gtTime;
	private String ltTime;
	
	public Login() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	/*日登录，去重统计
	 * db.runCommand({
			"distinct":"server",
			"key":"message.user_id",
			"query":{"message.type":"login"}
		})
	 */
	public int dayLoginAnalysis(){
		
		BasicDBObject distinct = new BasicDBObject();
		distinct.put("distinct", "server");
		distinct.put("key", "message.user_id");
		distinct.put("query", new BasicDBObject("message.type","login")
				.append("message.login_time", new BasicDBObject("$gt",gtTime).append("$lt", ltTime)));
		
		CommandResult resultSet = db.command(distinct);
		BasicBSONList values = (BasicBSONList)resultSet.get("values");
		
		int count = values.size();
		System.out.println(count);
		return count;
	}
	
	//登录次数,不去重
	public int loginAnalysis(){
		
		BasicDBObject query = new BasicDBObject();
		query.put("message.type", "login");
		query.put("message.login_time", new BasicDBObject("$gt",gtTime).append("$lt", ltTime));
		
		int count = collection.find(query).count();
		
		return count;
	}
	
	public void insertResult(){
		String sql = "insert into login_info (day_count,count,date) values (?,?,?)";
		PreparedStatement pstmt = null;
		
		gtTime = DateTimeUtil.dateCalculate(new Date(), -1)+" 23:59:59";
		ltTime = DateTimeUtil.dateCalculate(new Date(), 1)+" 00:00:00";
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			pstmt.setInt(1, dayLoginAnalysis());
			pstmt.setInt(2, loginAnalysis());
			pstmt.setDate(3, new java.sql.Date(new Date().getTime()));
			
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
		new Login().insertResult();
	}
}
