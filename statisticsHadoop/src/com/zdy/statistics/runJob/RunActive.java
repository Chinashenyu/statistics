package com.zdy.statistics.runJob;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class RunActive {

	public static void main(String[] args) throws IOException {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date nowDate = new Date();
		
		Configuration conf = new Configuration();
		conf.set("today", sdf.format(nowDate));//add today
		
		Job job = Job.getInstance(conf);
		
		//set inputpath
		for(int i=0;i>-30;i--){
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, i);
			String dateStr = sdf.format(calendar.getTime());
			
			try {
				Path path = new Path("hdfs://hadoop1:9000/statistics/qipai/login/login-"+dateStr+".dat");
				MultipleInputs.addInputPath(job, path, TextInputFormat.class);
			} catch (Exception e) {
				
			}
		}
		
		
		
	}

}
