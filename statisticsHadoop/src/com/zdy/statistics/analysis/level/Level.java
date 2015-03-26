package com.zdy.statistics.analysis.level;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.main.MainRun;

public class Level {

	private Connection connection;
	private DB db;
	private DBCollection collection;
	
	public Level() {
		connection = MysqlConnect.getConnection();
		db = MongoDBConnector.getDB();
		collection = db.getCollection("server");
	}
	
	/**
	 * mongodb脚本
	 * db.runCommand({"group":{
		"ns":"server",
		"key":{"message.level":true},
		"initial":{"count":0},
		"$reduce":function(doc,prev){
					prev.count++;
				},
		"condition":{"message.type":"level"}
		}})
	 * @return
	 */
	public String analysis(){
		
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		group.put("key", new BasicDBObject("message.level","true"));
		group.put("initial", new BasicDBObject("count",0));
		group.put("$reduce", "function(doc,prev){"+
							 "	prev.count++; "+
							 "}");
		group.put("condition", new BasicDBObject("message.type","level"));
		
		cmd.put("group", group);
		CommandResult commandResult = db.command(cmd);
		BasicBSONList retval = (BasicBSONList) commandResult.get("retval");
		
		Map<String,String> resMap = new HashMap<String, String>();
		if(retval != null){
			for (Object object : retval) {
				
				BasicDBObject dbObject = (BasicDBObject) object;
				String level = "等级"+((int)(double)dbObject.get("message.level"));
				Integer count = (int)(double)(dbObject.get("count"));
				
				resMap.put(level, count+" 人");
			}
			resMap.put("日期", new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		}
		
		return JSONObject.fromObject(resMap).toString();
	}
	
	public void insertResult(){
		String sql = " insert into level_info (result,date) values (?,?)";
		PreparedStatement pstmt = null;
		
		try {
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			pstmt.setString(1, analysis());
			pstmt.setDate(2, new java.sql.Date(new Date().getTime()));
			
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
	public static void main(String[] args) {
		new Level().insertResult();
	}
}
