package com.zdy.statistics.active;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ActiveMap2 extends Mapper<Text, LongWritable, Text, LongWritable> {
	
	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		
	}

}
