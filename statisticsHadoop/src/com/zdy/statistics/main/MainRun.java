package com.zdy.statistics.main;

public class MainRun {
	
	public MainRun(){
		
	}
	
	public static void init(){
		try {
			Class cla = Class.forName("com.zdy.statistics.analysis.common.MysqlConnect");
			Class eventContrastClass = Class.forName("com.zdy.statistics.analysis.contrastCache.EventContrast");
			Class gameContrastClass = Class.forName("com.zdy.statistics.analysis.contrastCache.GameContrast");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		init();
	}

}
