package com.zdy.statistics.main;

import com.zdy.statistics.analysis.common.MysqlConnect;

public class MainRun {

	
	
	public MainRun(){
		System.out.println("---");
	}
	
	public static void init(){
		
	}
	
	public static void main(String[] args) {
		MysqlConnect.getConnection();
	}

}
