package com.zdy.statistics.active;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ActiveReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		long count = 0;
		for (LongWritable value : values) {
			count += value.get();
		}
		if("1:30".equals(key.toString())){
			System.out.println("reducer===================== count : "+count+" ----- : "+key.toString());
		}
		//context.write(key, new LongWritable(count));
		
	}
	
}
