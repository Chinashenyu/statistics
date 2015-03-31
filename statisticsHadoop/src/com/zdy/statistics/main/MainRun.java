package com.zdy.statistics.main;

import org.apache.log4j.Logger;

import com.zdy.statistics.analysis.online.OnlineRMIServiceImpl;

public class MainRun {
	
	private static Logger logger = Logger.getLogger(MainRun.class);
	
	public MainRun(){
		
	}
	
	@SuppressWarnings({ "unused", "rawtypes" })
	public static void init(){
		try {
			//加载MySQL驱动
			Class cla = Class.forName("com.zdy.statistics.analysis.common.MysqlConnect");
			logger.info("加载MySQL驱动成功");
			//初始化记录事件类型的缓存
			Class eventContrastClass = Class.forName("com.zdy.statistics.analysis.contrastCache.EventContrast");
			logger.info("初始化--事件类型缓存成功");
			//初始化记录游戏标识的缓存
			Class gameContrastClass = Class.forName("com.zdy.statistics.analysis.contrastCache.GameContrast");
			logger.info("初始化--游戏标识缓存成功");
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
		logger.info("开启--定时器成功");
		
		//开启 在线 RMI 服务
		OnlineRMIServiceImpl.startRMIService();
		logger.info("开启--RMI服务成功");
	}

}
