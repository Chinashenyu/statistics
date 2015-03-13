package com.zdy.statistics.main;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimerRun {

	public void runTaskByDay(){
		
		Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
		calendar.set(Calendar.HOUR_OF_DAY, 16);
		calendar.set(Calendar.MINUTE, 40);
		calendar.set(Calendar.SECOND, 0);
//		calendar.set
		System.out.println(calendar.getTime());
		
		ScheduledExecutorService scheduledService = Executors.newScheduledThreadPool(20);
		
		scheduledService.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				System.out.println("-----------------------------"+new Date());
			}
		}, calendar.getTimeInMillis() - new Date().getTime(), (long) (1000*60*5), TimeUnit.MILLISECONDS);
		
	}
	
	public void runTaskByHour(){
		
	}
	
	public void runTaskByMinute(){
		
	}
	
	public static void main(String[] args) {
		TimerRun timerRun = new TimerRun();
		timerRun.runTaskByDay();
	}
}
