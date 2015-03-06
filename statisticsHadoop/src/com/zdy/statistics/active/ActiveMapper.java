package com.zdy.statistics.active;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

import com.zdy.statistics.util.DateTimeUtil;

public class ActiveMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	private Map<String,String> statisDateMap = new HashMap<String,String>();
	
	private Map<String,String> hasUserId = new HashMap<String,String>();
	
	private String today = "";
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//System.out.println("input path : "+((FileSplit)context.getInputSplit()).getPath().getName());
		
		String line = value.toString();//line:login,user_id,login_time
		line = line.replaceAll("\"", "");
		String[] loginValues = StringUtils.split(line,',');
		String userId = loginValues[1];
		String loginTime = loginValues[2].split(" ")[0];
		
		context.write(new Text("1:30"), new LongWritable(1));
		
//		if(loginValues[0] != null && "login".equals(loginValues[0])){
//			if(!hasUserId.containsKey(loginValues[1])){
//				hasUserId.put(userId, null);
//				
//				String keyStr = userId;
//				
//				int betweenDay = DateTimeUtil.calculateTowDay(today, loginTime);
//				if(betweenDay<=30 && betweenDay>15){
//					context.write(new Text(keyStr+":30"), new LongWritable(betweenDay));
//				}else if(betweenDay<=15 && betweenDay>7){
//					context.write(new Text(keyStr+":30"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":15"), new LongWritable(betweenDay));
//				}else if(betweenDay<=7 && betweenDay>5){
//					context.write(new Text(keyStr+":30"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":15"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":7"), new LongWritable(betweenDay));
//				}else if(betweenDay<=5 && betweenDay>3){
//					context.write(new Text(keyStr+":30"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":15"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":7"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":5"), new LongWritable(betweenDay));
//				}else{
//					context.write(new Text(keyStr+":30"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":15"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":7"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":5"), new LongWritable(betweenDay));
//					context.write(new Text(keyStr+":3"), new LongWritable(betweenDay));
//				}
//			}
//		}
		
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String d = conf.get("statisDate");
		today = conf.get("today");
	}
	
}
