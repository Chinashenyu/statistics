package com.zdy.statistics.util;

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
	
//	public static String formatTODate(String dateTime){
//		
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		return sdf.format(date);
//	}
	
	public static void main(String[] args) {
		System.out.println(getyesterday());
//		System.out.println(formatTODate("2015-03-03 23:45:09"));
	}
	
}
