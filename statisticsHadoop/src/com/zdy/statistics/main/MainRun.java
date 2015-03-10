package com.zdy.statistics.main;

public class MainRun {
	
	public MainRun(){
		
	}
	
	public static void init(){
		try {
			Class cla = Class.forName("com.zdy.statistics.analysis.common.MysqlConnect");
			Class eventContrastClass = Class.forName("com.zdy.statistics.analysis.common.AnalysisEventContrast");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		init();
	}

}
