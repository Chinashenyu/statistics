package com.zdy.statistics.analysis.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.zdy.statistics.analysis.common.MysqlConnect;

public class BaseDaoImpl {

	private Connection connection;
	
	public BaseDaoImpl() {
		connection = MysqlConnect.getConnection();
	}
	
	public void insert(PreparedStatement pstmt){
//		if(){
//			
//		}
	}
	
}
