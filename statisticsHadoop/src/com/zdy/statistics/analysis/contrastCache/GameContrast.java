package com.zdy.statistics.analysis.contrastCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.zdy.statistics.analysis.common.MysqlConnect;

public class GameContrast {

	private static Connection connection;
	
	private static Map<Integer, String> gameMap = new HashMap<Integer, String>();
	
	private GameContrast(){}
	
	static{
		initialGameMap(null);
	}
	
	private static void initialGameMap(Integer[] gameIds){
		
		String sql = " select game_type,game_name from gametype_contrast";
		PreparedStatement pstmt = null;
		if(gameIds != null && gameIds.length > 0){
			sql += " where game_type not in "+Arrays.toString(gameIds).replaceAll("[", "(").replaceAll("]", ")");
		}
		
		connection = MysqlConnect.getConnection();
		try {
			pstmt = connection.prepareStatement(sql);
			ResultSet resultSet = pstmt.executeQuery();
			while(resultSet.next()){
				gameMap.put(resultSet.getInt(1), resultSet.getString(2));
			}
		} catch (SQLException e) {
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
	
	public static void updateGameMap(){
		Integer[] gameIds = new Integer[gameMap.size()];
		gameIds = gameMap.keySet().toArray(gameIds);
	}
	
	public static Map<Integer,String> getGameMap(){
		return gameMap;
	}
}
