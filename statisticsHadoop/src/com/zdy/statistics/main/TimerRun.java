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

	public void runScheduled(long initialDelay,long period,TimeUnit timeUnit,Map<String,String> classMap,final Map<String,Object[]> argesMap){

		ScheduledExecutorService scheduledService = Executors.newScheduledThreadPool(10);
		
		for(Entry<String, String> entry : classMap.entrySet()){
			final String className = entry.getKey();//class name
			final String methodName = entry.getValue();//called method name;
			
			scheduledService.scheduleAtFixedRate(new Runnable() {
				
				@SuppressWarnings("rawtypes")
				@Override
				public void run() {
					try {
						Class clazz = Class.forName(className);
						Method[] methods = clazz.getMethods();
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
		analysisClasses.put("com.zdy.statistics.analysis.register.Register", "insertResult()");
		
		Map<String,Object[]> argesMap = new HashMap<String, Object[]>();
		argesMap.put("com.zdy.statistics.analysis.register.Register", null);
		
		Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
		calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH)+1);
		calendar.set(Calendar.HOUR_OF_DAY, 2);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		
		long initialDelay = calendar.getTimeInMillis() - new Date().getTime();
		long period = 1000*60*60*24;
		TimeUnit timeUnit = TimeUnit.MILLISECONDS;
		
		runScheduled(initialDelay, period, timeUnit, analysisClasses, argesMap);
		
		/*
		 * 2.启动一小时定时器
		 */
		
		/*
		 * 3.启动每五分钟定时器
		 */
	}
	
	public static void main(String[] args) {
		TimerRun timerRun = new TimerRun();
		timerRun.startTimer();
	}
}
