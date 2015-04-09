package com.zdy.statistics.analysis.register;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.util.DateTimeUtil;

public class Register {

	private DB db;
	private DBCollection collection;
	
	private Logger logger = Logger.getLogger(Register.class);
	
	public Register() {
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	public int[] analysis(){
		int[] counts = new int[2];
		
		int dayRegisterCount = dayRegisterAnalysis();
		int totalDevice = totalDeviceAnalysis();
		
		counts[0] = dayRegisterCount;
		counts[1] = totalDevice;
		return counts;
	}
	
	//每日新增注册
	public int dayRegisterAnalysis(){
		BasicDBObject query = new BasicDBObject();
		query.put("message.type", "registe");
		String gtTime = DateTimeUtil.dateCalculate(new Date(), -1)+" 00:00:00";
		String ltTime = DateTimeUtil.dateCalculate(new Date(), -1)+" 23:59:59";
		query.put("message.registe_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime));
		
		int dayRegisterCount = collection.find(query).count();
		return dayRegisterCount;
	}
	
	/*
	 * 总设备
	 * db.runCommand({"group":{
			"ns":"server",
			"key":{"message.dev_id":true},
			"initial":{"count":0},
			"$reduce":function(doc,prev){
				prev.count += 1;
			},
			"condition":{"message.type":"register"}
		}})
	 */
	public int totalDeviceAnalysis(){
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.dev_id","true"));
		group.put("initial", new BasicDBObject("count",0));
		group.put("$reduce", "function(doc,prev){"+
							 	"prev.count += 1;"+
							 "}");
		group.put("condition", new BasicDBObject("message.type","registe"));
		
		cmd.put("group", group);
		CommandResult resultSet = db.command(cmd);
		BasicBSONList retval = (BasicBSONList) resultSet.get("retval");
		int total = retval.size();
		
		return total;
	}
	
	public void insertResult(){
		Connection connection = null;
		
		String sql = " insert into register_info (register_count,new_add,total_device,new_device,day_time) select register_count+?,?,?,?-total_device,? from register_info where day_time = ?";
		PreparedStatement pstmt = null;
		
		int[] counts = analysis();
		
		try {
			connection = MysqlConnect.getConnection();
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			pstmt.setInt(1, counts[0]);
			pstmt.setInt(2, counts[0]);
			pstmt.setInt(3, counts[1]);
			pstmt.setInt(4, counts[1]);
			pstmt.setString(5, DateTimeUtil.dateCalculate(new Date(), -1));
			pstmt.setString(6, DateTimeUtil.dateCalculate(new Date(), -2));
			
			pstmt.executeUpdate();
			connection.commit();
//			System.out.println(pstmt.toString());
			logger.info("注册：新增注册信息成功！");
		} catch (SQLException e) {
			try {
				connection.rollback();
				logger.info("注册：新增注册信息失败！！！！！");
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
		new Register().insertResult();
	}
}
