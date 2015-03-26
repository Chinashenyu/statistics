package com.zdy.statistics.main;

import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimerRun {

	//定时调度
	public void runScheduled(long initialDelay,long period,TimeUnit timeUnit,Map<String,String> classMap,final Map<String,Object[]> argesMap){

		//锁
		Object lock = null;
		
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
								method.invoke(arges);
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
		/*
		 * 1.启动隔天查询定时器
		 */
		Map<String,String> analysisClasses = new HashMap<String, String>();
		analysisClasses.put("com.zdy.statistics.analysis.register.Register", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.firstConsume.FirstConsume", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.gameJoin.GameJoin", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.huanle.AnalysisHuanLe", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.level.Level", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.login.Login", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.recharge.AnalysisRecharge", "insertResult");
		analysisClasses.put("com.zdy.statistics.analysis.shop.AnalysisShop", "execShopAnalysis");
		analysisClasses.put("com.zdy.statistics.analysis.userInfo.UserInfo", "insertResult");
		
		Map<String,Object[]> argesMap = new HashMap<String, Object[]>();
		argesMap.put("com.zdy.statistics.analysis.register.Register", null);
		argesMap.put("com.zdy.statistics.analysis.firstConsume.FirstConsume", null);
		argesMap.put("com.zdy.com.zdy.statistics.analysis.gameJoin.GameJoin", null);
		argesMap.put("com.zdy.statistics.analysis.huanle.AnalysisHuanLe", null);
		argesMap.put("com.zdy.statistics.analysis.level.Level", null);
		argesMap.put("com.zdy.statistics.analysis.login.Login", null);
		argesMap.put("com.zdy.statistics.analysis.recharge.AnalysisRecharge", null);
		argesMap.put("com.zdy.statistics.analysis.shop.AnalysisShop", null);
		argesMap.put("", null);
		argesMap.put("", null);
		
		//隔天的毫秒数
		Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
		calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH)+1);
		calendar.set(Calendar.HOUR_OF_DAY, 2);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		
		long initialDelay = calendar.getTimeInMillis() - new Date().getTime();
		long period = 1000*60*60*24;
		TimeUnit timeUnit = TimeUnit.MILLISECONDS;
		
		//启动定时器
		runScheduled(initialDelay, period, timeUnit, analysisClasses, argesMap);
		
		/*
		 * 2.启动一小时定时器
		 */
		Map<String,String> analysisClassesHour = new HashMap<String, String>();
		
		Map<String,Object[]> argesMapHour = new HashMap<String, Object[]>();
		
		//隔一小时的毫秒数
		Calendar calendarHour = Calendar.getInstance(TimeZone.getDefault());
		calendarHour.set(Calendar.HOUR_OF_DAY, calendarHour.get(Calendar.HOUR_OF_DAY)+1);
		calendarHour.set(Calendar.MINUTE, 0);
		calendarHour.set(Calendar.SECOND, 0);
		
		long initialDelayHour = calendar.getTimeInMillis() - new Date().getTime();
		long periodHour = 1000*60*60;
		TimeUnit timeUnitHour = TimeUnit.MILLISECONDS;
		
		runScheduled(initialDelayHour, periodHour, timeUnitHour, analysisClassesHour, argesMapHour);
		/*
		 * 3.启动每五分钟定时器
		 */
	}
	
	public static void main(String[] args) {
		TimerRun timerRun = new TimerRun();
		timerRun.startTimer();
	}
}
