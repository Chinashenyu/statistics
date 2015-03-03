package com.zdy.statistics.runJob;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;

public class RunActive {

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
//		conf.setClass(name, theClass, xface);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		Date nowDate = new Date();
		for(int i=0;i>-30;i--){
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, i);
			String dateStr = sdf.format(calendar.getTime());
			
			
		}
		
	}

}
