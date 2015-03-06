package com.zdy.statistics.active;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ActiveJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date nowDate = new Date();
		
		Configuration conf = new Configuration();
		conf.set("today", sdf.format(nowDate));//add today
		conf.set("mapreduce.job.jar", "my.jar");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ActiveJob.class);
		
		
		job.setMapperClass(ActiveMapper.class);
		job.setCombinerClass(ActiveCombiner.class);
		job.setReducerClass(ActiveReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//set inputpath
		for(int i=0;i>-30;i--){
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, i);
			String dateStr = sdf.format(calendar.getTime());
			
			try {
				Path path = new Path("hdfs://hadoop1:9000/statistics/qipai/login/login-"+dateStr+".dat");
				MultipleInputs.addInputPath(job, path, TextInputFormat.class,ActiveMapper.class);
			} catch (Exception e) {
				
			}
		}
		
		//set outputpath
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/statistics/qipai/login/active"));
		
//		JobControl jc = new JobControl("active");
//		jc.addJob(job);
		
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		try {
			int result = ToolRunner.run(new Configuration(), new ActiveJob(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
