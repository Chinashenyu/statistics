package com.zdy.statistics.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTimeUtil {

	/**
	 * Example,today is 2015-02-28,return 2015-02-27
	 * @return get yesterday date;
	 */
	public static String getyesterday(){
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_MONTH, -1);//the numbers of days of month minus one; get yesterday date;
		
		String yesterday = sdf.format(calendar.getTime());
		
		
		return yesterday;
	}
	
	/**
	 * 
	 * @param date1
	 * @param date2
	 * @return the mnuber of days between tow dates;
	 */
	public static int calculateTowDay(String date1,String date2){
		
		int betweenDays = 0;
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date d1 = sdf.parse(date1);
			Date d2 = sdf.parse(date2);
			long d = d1.getTime()-d2.getTime();
			betweenDays = (int)d/1000/60/60/24;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return betweenDays;
	}
	
	/**
	 * date add or minus
	 * @param date
	 * @param days
	 * @return the date after add or minus ;format : yyyy-MM-dd
	 */
	public static String dateCalculate(Date date,int days){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, days);
		
		return sdf.format(calendar.getTime());
	}
	
	
	public static String hourCalculate(Date date,int hour){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.HOUR_OF_DAY, hour);
		
		return sdf.format(calendar.getTime());
	}
	
	public static String minuteCalculate(Date date,int minute){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MINUTE, minute);
		
		return sdf.format(calendar.getTime());
	}
	
	public static void main(String[] args) {
		System.out.println(minuteCalculate(new Date(),-15));
	}
	
	
}
