package com.zdy.statistics.main;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class TimerRun {

	private Logger logger = Logger.getLogger(TimerRun.class);
	
	//定时调度
	public void runScheduled(long initialDelay,long period,TimeUnit timeUnit,Map<String,String> classMap,final Map<String,Object[]> argesMap){

		//线程池
		ScheduledExecutorService scheduledService = Executors.newScheduledThreadPool(20);
		
		for(Entry<String, String> entry : classMap.entrySet()){
			final String className = entry.getKey();//class name
			final String methodName = entry.getValue();//called method name;
			
			scheduledService.scheduleAtFixedRate(new Runnable() {
				
				@SuppressWarnings("rawtypes")
				@Override
				public void run() {
					try {
						//得到任务类
						Class clazz = Class.forName(className);
						Method[] methods = clazz.getMethods();
						//判断调用的方法
						for (Method method : methods) {
							if(method.getName().equals(methodName)){
								Object[] arges = argesMap.get(className);
								if(arges == null){
									arges = new Object[0];
								}
								method.invoke(clazz.newInstance(),null);
								logger.info(className+" -- "+methodName+" : 执行");
								break;
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}, initialDelay, period, timeUnit);
			
		}
	}
	
	public void startTimer(){
		//------ 1.启动隔天查询定时器 -------------------------------------------------------------------------------------//
		Map<String,String> analysisClasses = new HashMap<String, String>();
		analysisClasses.put("com.zdy.statistics.analysis.register.Register", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.firstConsume.FirstConsume", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.gameJoin.GameJoin", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.huanle.AnalysisHuanLe", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.level.Level", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.login.Login", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.recharge.AnalysisRecharge", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.shop.AnalysisShop", "execShopAnalysis");
		analysisClasses.put("com.zdy.statistics.analysis.userInfo.UserInfo", "userBasicInfo");
		analysisClasses.put("com.zdy.statistics.analysis.userInfo.UserInfo", "analysisLevel");
		analysisClasses.put("com.zdy.statistics.analysis.userInfo.UserInfo", "analysisHuanle");
		analysisClasses.put("com.zdy.statistics.analysis.userInfo.UserInfo", "ananlysisRecharge");
		analysisClasses.put("com.zdy.statistics.analysis.userInfo.UserInfo", "analysisGameJoin");
		
		Map<String,Object[]> argesMap = new HashMap<String, Object[]>();
		argesMap.put("com.zdy.statistics.analysis.register.Register", null);
		argesMap.put("com.zdy.statistics.analysis.firstConsume.FirstConsume", null);
		argesMap.put("com.zdy.com.zdy.statistics.analysis.gameJoin.GameJoin", null);
		argesMap.put("com.zdy.statistics.analysis.huanle.AnalysisHuanLe", null);
		argesMap.put("com.zdy.statistics.analysis.level.Level", null);
		argesMap.put("com.zdy.statistics.analysis.login.Login", null);
		argesMap.put("com.zdy.statistics.analysis.recharge.AnalysisRecharge", null);
		argesMap.put("com.zdy.statistics.analysis.shop.AnalysisShop", null);
		argesMap.put("com.zdy.statistics.analysis.userInfo.UserInfo", null);
		
		Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
		calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH)+1);
		calendar.set(Calendar.HOUR_OF_DAY, 2);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		
		long initialDelay = calendar.getTimeInMillis() - new Date().getTime();
		long period = 1000*60*60*24;
		TimeUnit timeUnit = TimeUnit.MILLISECONDS;
		
		runScheduled(initialDelay, period, timeUnit, analysisClasses, argesMap);
		logger.info("启动隔天查询定时器");
		
		
		//------2.启动一小时定时器-------------------------------------------------------------------------------------//
		Map<String,String> analysisClassesHour = new HashMap<String, String>();
		analysisClassesHour.put("com.zdy.statistics.analysis.register.RegisterSummit", "insertResult");
		
		Map<String,Object[]> argesMapHour = new HashMap<String, Object[]>();
		argesMapHour.put("com.zdy.statistics.analysis.register.RegisterSummit", null);
		
		Calendar calendarHour = Calendar.getInstance(TimeZone.getDefault());
		calendarHour.set(Calendar.HOUR_OF_DAY, calendarHour.get(Calendar.HOUR_OF_DAY)+1);
		calendarHour.set(Calendar.MINUTE, 0);
		calendarHour.set(Calendar.SECOND, 0);
		
		long initialDelayHour = calendarHour.getTimeInMillis() - new Date().getTime();
		long periodHour = 1000*60*60;
		TimeUnit timeUnitHour = TimeUnit.MILLISECONDS;
		
		runScheduled(initialDelayHour, periodHour, timeUnitHour, analysisClassesHour, argesMapHour);
		logger.info("启动一小时定时器");
		
		//------ 4.启动每十五分钟定时器 -------------------------------------------------------------------------------------//
		Map<String,String> analysisClassesFifteen = new HashMap<String, String>();
		analysisClassesFifteen.put("com.zdy.statistics.analysis.login.LoginSummit", "insertResult");
		
		Map<String,Object[]> argesMapFifteen = new HashMap<String, Object[]>();
		argesMapFifteen.put("com.zdy.statistics.analysis.login.LoginSummit", null);
		
		Calendar calendarFifteen = Calendar.getInstance(TimeZone.getDefault());
		calendarFifteen.set(Calendar.MINUTE, 15*((Calendar.MINUTE/15)+1));
		calendarFifteen.set(Calendar.SECOND, 0);
		
		long initialDelayFifteen = calendarFifteen.getTimeInMillis() - new Date().getTime();
		long periodFifteen = 1000*60*15;
		TimeUnit timeUnitFifteen = TimeUnit.MILLISECONDS;
		
		runScheduled(initialDelayFifteen, periodFifteen, timeUnitFifteen, analysisClassesFifteen, argesMapFifteen);
		logger.info("启动每十五分钟定时器");
			
		//------ 5.启动每五分钟定时 -------------------------------------------------------------------------------------//
		Map<String,String> analysisClassesFive = new HashMap<String, String>();
		analysisClassesFive.put("com.zdy.statistics.analysis.online.OnlineAnalysis", "analysisOnlineCount");
		
		Map<String,Object[]> argesMapFive = new HashMap<String, Object[]>();
		argesMapFive.put("com.zdy.statistics.analysis.online.OnlineAnalysis", null);
		
		Calendar calendarFive = Calendar.getInstance(TimeZone.getDefault());
		calendarFive.set(Calendar.MINUTE, 5*((Calendar.MINUTE/5)+1));
		calendarFive.set(Calendar.SECOND, 0);
		
		long initialDelayFive = calendarFive.getTimeInMillis() - new Date().getTime();
		long periodFiven = 1000*60*15;
		TimeUnit timeUnitFive = TimeUnit.MILLISECONDS;
		
		runScheduled(initialDelayFive, periodFiven, timeUnitFive, analysisClassesFive, argesMapFive);
		logger.info("启动每五分钟定时");
		
		//------ 6.每隔五秒总 -------------------------------------------------------------------------------------//
		Map<String,String> analysisClassesFiveSecond = new HashMap<String, String>();
		analysisClassesFiveSecond.put("com.zdy.statistics.analysis.online.OnlineAnalysis", "analysis");
		
		Map<String,Object[]> argesMapFiveSecond = new HashMap<String, Object[]>();
		argesMapFiveSecond.put("com.zdy.statistics.analysis.online.OnlineAnalysis", null);
		
		Calendar calendarFiveSecond = Calendar.getInstance(TimeZone.getDefault());
		calendarFiveSecond.set(Calendar.SECOND, 5*((Calendar.SECOND/5)+1));
		
		long initialDelayFiveSecond = calendarFiveSecond.getTimeInMillis() - new Date().getTime();
		long periodFiveSecond = 1000*5;
		TimeUnit timeUnitFiveSecond = TimeUnit.MILLISECONDS;
		
		runScheduled(initialDelayFiveSecond, periodFiveSecond, timeUnitFiveSecond, analysisClassesFiveSecond, argesMapFiveSecond);
		logger.info("启动每五秒钟定时");
	}
	
	public static void main(String[] args) {
		TimerRun timerRun = new TimerRun();
		timerRun.startTimer();
	}
}
