package com.zdy.statistics.main;

public class MainRun {
	
	public MainRun(){
		
	}
	
	@SuppressWarnings({ "unused", "rawtypes" })
	public static void init(){
		try {
			//加载MySQL驱动
			Class cla = Class.forName("com.zdy.statistics.analysis.common.MysqlConnect");
			//初始化记录事件类型的缓存
			Class eventContrastClass = Class.forName("com.zdy.statistics.analysis.contrastCache.EventContrast");
			//初始化记录游戏标识的缓存
			Class gameContrastClass = Class.forName("com.zdy.statistics.analysis.contrastCache.GameContrast");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		//初始化系统数据
		init();
		
		//开启定时器
		TimerRun timer = new TimerRun();
		timer.startTimer();
	}

}
