package com.zdy.statistics.analysis.contrastCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.zdy.statistics.analysis.common.MysqlConnect;

public class EventContrast {

	private static Connection connection;
	
	private static Map<Integer,String> eventMap = new HashMap<Integer, String>();
	
	private EventContrast(){
	}
	
	static{
		initEventMap(null);
	}
	
	private static void initEventMap(Integer[] eventIds) {
		
		String sql = " select event_id ,event_name from event_contrast ";
		PreparedStatement pstmt = null;
		
		if(eventIds != null && eventIds.length > 0){
			sql += " where event_id not in "+Arrays.toString(eventIds).replace("[", "(").replace("]", ")");
		}
		
		connection = MysqlConnect.getConnection();
		try {
			System.out.println(sql);
			pstmt = connection.prepareStatement(sql);
			ResultSet resultSet = pstmt.executeQuery();
			while(resultSet.next()){
				eventMap.put(resultSet.getInt(1), resultSet.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				if(pstmt != null){
					pstmt.close();
				}
				if(connection != null){
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void updateEventMap(){
		Integer[] ids = new Integer[eventMap.size()];
		ids = eventMap.keySet().toArray(ids);
		
		initEventMap(ids);
	}
	
	public static Map<Integer,String> getEventMap(){
		return eventMap;
	}
	
	public static void main(String[] args) {
		System.out.println(eventMap);
	}
	
}
