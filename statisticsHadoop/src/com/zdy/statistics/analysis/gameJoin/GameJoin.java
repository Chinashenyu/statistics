package com.zdy.statistics.analysis.gameJoin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.zdy.statistics.analysis.common.MongoDBConnector;
import com.zdy.statistics.analysis.common.MysqlConnect;
import com.zdy.statistics.analysis.contrastCache.GameContrast;
import com.zdy.statistics.util.DateTimeUtil;

public class GameJoin {

	private DB db;
	
	public GameJoin() {
		db = MongoDBConnector.getDB();
	}
	
	public String analysisGameTable(int type){
		
		/*
		 * db.runCommand({"group":{
			"ns":"server",
			"key":{"message.compete":true,"message.table_id":true},
			"initial":{"count":0},
			"$reduce":function(doc,prev){
				 prev.count += 1;
				},
			"condition":{"message.type":"join"}
			}})
		 */
		BasicDBObject cmd = new BasicDBObject();
		
		BasicDBObject group = new BasicDBObject();
		group.put("ns", "server");
		if(type == 1 || type ==2){
			group.put("key", new BasicDBObject("message.game","true").append("message.table_id", "true"));
			group.put("initial", new BasicDBObject("count",0));
			group.put("$reduce", "function(doc,prev){prev.count += 1;}");
		}else if(type == 3 || type == 4){
			group.put("key", new BasicDBObject("message.competeName","true").append("message.table_id", "true"));
			group.put("initial", new BasicDBObject("count",0).append("name", ""));
			group.put("$reduce", "function(doc,prev){prev.count += 1;prev.name = doc.message.competeName}");
		}
		
		String gtTime = DateTimeUtil.dateCalculate(new Date(), -1)+" 00:00:00";
		String ltTime = DateTimeUtil.dateCalculate(new Date(), -1)+" 23:59:59";
		if(type == 1 || type == 2){
			group.put("condition", new BasicDBObject("message.type","join")
			.append("message.join_time", new BasicDBObject("$gte",gtTime).append("$lte", ltTime)));
		}else if(type == 3 || type == 4){
			group.put("condition", new BasicDBObject("message.type","join")
					.append("message.compete", new BasicDBObject("$ne",null))
					.append("message.join_time", new BasicDBObject("$lte", ltTime)));
		}
		
		cmd.put("group", group);
		System.out.println(cmd);
		CommandResult cmdResult = db.command(cmd);
		
		Map<String,Object> resMap = new HashMap<String, Object>();
		Map<Integer,String> gameMap = GameContrast.getGameMap();
		BasicBSONList retval = (BasicBSONList) cmdResult.get("retval");
		for (Object object : retval) {
			BasicDBObject dbObject = (BasicDBObject) object;
			
			double keyD = 0;
			if(type == 3 || type == 4)
				;
			else if(type == 1 || type == 2)
				keyD = (Double)dbObject.get("message.game");
			Integer joinCount = (int)((double)dbObject.get("count"));
			Integer key = (int)keyD;
			
			String gameName = "";
			if(type == 1 || type == 2){
				if(gameMap.containsKey(key)){
					gameName = gameMap.get(key);
				}else{
					GameContrast.updateGameMap();
					gameName = gameMap.get(key);
				}
			}else{
				gameName = dbObject.getString("name");
			}
			
			if(resMap.containsKey(gameName)){
				if(type == 1 || type == 3){
					if(type == 3) gameName = dbObject.getString("name");
					resMap.put(gameName, (Integer)resMap.get(gameName)+1);
				}else if(type == 2 || type == 4){
					resMap.put(gameName, (Integer)resMap.get(gameName)+joinCount);
				}
			}else{
				if(type == 1 || type == 3){
					if(type == 3) gameName = dbObject.getString("name");
					resMap.put(gameName, 1);
				}else if(type == 2 || type == 4){
					resMap.put(gameName,  joinCount);
				}
			}
			
		}
		
		if(type == 3){
			StringBuffer buffer = new StringBuffer();
			int i = 0;
			for(Entry<String, Object> entry : resMap.entrySet()){
				String key = entry.getKey();
				Object value = entry.getValue();
				
				if(i == resMap.size() -1){
					buffer.append(key+"："+value);
				}else{
					buffer.append(key+"："+value+" , ");
				}
				i++;
			}
			Map<String,String> jsonMap = new HashMap<String, String>();
			jsonMap.put("content", buffer.toString());
			jsonMap.put("日期", DateTimeUtil.getyesterday());
			return JSONObject.fromObject(jsonMap).toString();
		}else{
			resMap.put("日期", DateTimeUtil.getyesterday());
			return JSONObject.fromObject(resMap).toString();
			
		}
	}
	
	public void insertResult(){
		Connection connection = null;
		
		String sql = " insert into game_join (result_set,type,date) values (?,?,?)";
		PreparedStatement pstmt = null;
		
		try {
			connection = MysqlConnect.getConnection();
			connection.setAutoCommit(false);
			pstmt = connection.prepareStatement(sql);
			for(int i=1;i<=4;i++){
				String resJson = analysisGameTable(i);
				if(resJson != null){
					pstmt.setString(1, resJson);
				}
				pstmt.setInt(2, i);
				pstmt.setString(3, DateTimeUtil.dateCalculate(new Date(), -1));
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
			try {
				pstmt.close();
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		new GameJoin().insertResult();
//		new GameJoin().analysisGameTable(4);
	}
}
