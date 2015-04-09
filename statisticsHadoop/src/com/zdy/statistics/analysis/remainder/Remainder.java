package com.zdy.statistics.analysis.remainder;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class Remainder {

	private DB db;
	private DBCollection collection;

	private Date nowDaTe = new Date();
	private String yestoday = DateTimeUtil.dateCalculate(nowDaTe, -1);
	private String startTime = yestoday + " 00:00:00";
	private String endTime = yestoday + " 23:59:59";
	
	public Remainder() {
		db = MongoDBConnector.getDB();
		
		collection = db.getCollection("server");
		
		
	}

	// 查找今日新注册用户
	public void initAnalysis() {
		
		// 查找今日新注册用户
		BasicDBObject query = new BasicDBObject();

		query.put("message.type", "registe");
		query.put("message.registe_time",
				new BasicDBObject("$gte", startTime).append("$lte", endTime));

		DBCursor cursor = collection.find(query);

		// 今日新注册用户id
		List<Object> userIds = new ArrayList<Object>();

		while (cursor.hasNext()) {
			BasicDBObject object = (BasicDBObject) cursor.next();
			DBObject message = (DBObject) object.get("message");
			userIds.add(message.get("user_id"));
		}

		Connection connection = null;
		
		String insertRegistUseriIdSQL = " insert into remainder (frist_registe,date) values (?,?)";
		PreparedStatement pstmt = null;

		try {
			connection = MysqlConnect.getConnection();
			connection.setAutoCommit(false);
			
			pstmt = connection.prepareStatement(insertRegistUseriIdSQL);
			pstmt.setString(1, userIds.toString());
			pstmt.setString(2, yestoday);

			pstmt.executeUpdate();
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(pstmt != null){try { pstmt.close(); } catch (SQLException e) { }}
			if(connection != null){try { connection.close(); } catch (SQLException e) { }}
		}

	}

	public void analysis(String type,String remaindeDate) {
		Connection connection = null;

		//查找某日留存
		String querySQL = " select * from remainder where date = '"+remaindeDate+"'";
		PreparedStatement queryPstmt = null;
		String[] uids = null;
		
		try {
			connection = MysqlConnect.getConnection();
			queryPstmt = connection.prepareStatement(querySQL);
			ResultSet resultSet = queryPstmt.executeQuery();
			
			String userIdsStr = "";
			while (resultSet.next()) {
				// 当日注册的用户id
				userIdsStr = resultSet.getString(2);
			}
			if("".equals(userIdsStr))
				return ;
			uids = userIdsStr.replaceAll("\\[", "")
					.replaceAll("\\]", "").split(",");
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(queryPstmt != null){try { queryPstmt.close(); } catch (SQLException e) { }}
		}
		
		if(uids != null && uids.length > 0){
			
			// 去重查询 当日注册，今日登陆的用户
			BasicDBObject distinct = new BasicDBObject();
			distinct.put("distinct", "server");
			distinct.put("key", "message.user_id");

			BasicDBList in = new BasicDBList();
			for (String uid : uids) {
				if(uid.contains("E")){
					System.out.println(uid.trim());
					BigDecimal bd = new BigDecimal(uid.trim());
					uid = bd.toPlainString();
				}
				in.add(Integer.parseInt(uid.trim()));
			}

			distinct.put("query",new BasicDBObject("message.type", "login")
								.append("message.user_id",new BasicDBObject("$in", in))
								.append("message.login_time", new BasicDBObject("$gte", startTime).append("$lte", endTime)));

			CommandResult cmdResultSet = db.command(distinct);
			BasicBSONList values = (BasicBSONList) cmdResultSet.get("values");

			double count = values.size();

			// 留存率
			double remainde = 0d;
			if (uids.length == 0) {
				remainde = 0;
			} else {
				remainde = count / (double)uids.length;
			}
			
			DecimalFormat df = new DecimalFormat("#.00");
			
			//更新留存率
			String updateSQL = " update remainder set " + type + " = " + df.format(remainde) + " where date ='" + remaindeDate + "'";
			PreparedStatement updatePstmt = null;
			try {
				connection.setAutoCommit(false);
				updatePstmt = connection.prepareStatement(updateSQL);
				updatePstmt.executeUpdate();
				
				connection.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				if(updatePstmt != null){try { updatePstmt.close(); } catch (SQLException e) { }}
				if(connection != null){try { connection.close(); } catch (SQLException e) { }}
			}
			
		}
		
		
		
	}
	
	public void insertResult(){
		
		initAnalysis();
		
		// 日期 向前推，计算出前 次、三、七、三十日的 日期
		String tow = DateTimeUtil.dateCalculate(nowDaTe, -2);
		String three = DateTimeUtil.dateCalculate(nowDaTe, -3);
		String seven = DateTimeUtil.dateCalculate(nowDaTe, -7);
		String thirty = DateTimeUtil.dateCalculate(nowDaTe, -30);
		
		Map<String,String> remaindeMap = new HashMap<String, String>(); 
		remaindeMap.put("tow", tow);
		remaindeMap.put("three", three);
		remaindeMap.put("seven", seven);
		remaindeMap.put("thirty", thirty);
		
		for (Entry<String, String> entry : remaindeMap.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			System.out.println(key+" : "+value);
			
			analysis(key, value);
		}
	}

	public static void main(String[] args) {
		new Remainder().insertResult();
	}

}
