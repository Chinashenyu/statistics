package com.zdy.statistics.active;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class ActiveMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{

	private Map<String,String> statisDateMap = new HashMap<String,String>();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		line.replaceAll("\"", "");
		String[] loginValues = StringUtils.split(line,',');
		
		String keyStr = loginValues[1]+":"+loginValues[2].split(" ")[0]+"";
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String d = conf.get("statisDate");
		String today = conf.get("today");
	}
	
}
