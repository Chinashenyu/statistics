package com.zdy.statistics.analysis.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AnalysisEventContrast {

	private static Connection connection;
	
	private static Map<Integer,String> eventMap = new HashMap<Integer, String>();
	
	static{
		initEventMap(null);
	}
	
	public static void initEventMap(Integer[] eventIds) {
		
		String sql = " select event_id ,event_name from event_contrast ";
		
		if(eventIds != null && eventIds.length > 0){
			sql += " where event_id not in "+Arrays.toString(eventIds).replace("[", "(").replace("]", ")");
		}
		
		connection = MysqlConnect.getConnection();
		try {
			PreparedStatement pstmt = connection.prepareStatement(sql);
			ResultSet resultSet = pstmt.executeQuery();
			while(resultSet.next()){
				eventMap.put(resultSet.getInt(1), resultSet.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void updateEventMap(){
		Integer[] ids = new Integer[eventMap.size()];
		ids = eventMap.entrySet().toArray(ids);
		
		initEventMap(ids);
	}
	
	public static Map getEventMap(){
		return eventMap;
	}
	
	public static void main(String[] args) {
		System.out.println(eventMap);
	}
	
}
