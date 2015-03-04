package com.zdy.statistics.active;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ActiveCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		Text newKey = new Text(key.toString().split(":")[1]);
		
		long count = 0L;
		for(LongWritable value : values){
			count += value.get();
		}
		switch (newKey.toString()) {
		case "30":
			if(count == 465){
				context.write(newKey, new LongWritable(1));
			}
			break;
		case "15":
			if(count == 120){
				context.write(newKey, new LongWritable(1));
			}
			break;
		case "7":
			if(count == 28){
				context.write(newKey, new LongWritable(1));
			}
			break;
		case "5":
			if(count == 15){
				context.write(newKey, new LongWritable(1));
			}
			break;
		case "3":
			if(count == 6){
				context.write(newKey, new LongWritable(1));
			}
			break;

		default:
			break;
		}
	}
	
}
