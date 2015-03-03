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
	
//	public static String formatTODate(String dateTime){
//		
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		return sdf.format(date);
//	}
	
	public static void main(String[] args) {
		System.out.println(getyesterday());
		System.out.println(calculateTowDay("2015-03-05", "2015-03-04"));
	}
	
}
