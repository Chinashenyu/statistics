package com.zdy.statistics.hadoop.active;

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
		if("1:30".equals(key.toString())){
			System.out.println("===================== count : "+count+" ----- : "+key.toString());
		}
		switch (newKey.toString()) {
		case "30":
			if(count == 465){
				System.out.println("====================="+count);
				context.write(newKey, new LongWritable(1));
			}
			break;
		case "15":
			if(count == 120){
				System.out.println("====================="+count);
				context.write(newKey, new LongWritable(1));
			}
			break;
		case "7":
			if(count == 28){
				System.out.println("====================="+count);
				context.write(newKey, new LongWritable(1));
			}
			break;
		case "5":
			if(count == 15){
				System.out.println("====================="+count);
				context.write(newKey, new LongWritable(1));
			}
			break;
		case "3":
			if(count == 6){
				System.out.println("====================="+count);
				context.write(newKey, new LongWritable(1));
			}
			break;

		default:
			System.out.println("====================="+count);
			break;
		}
	}
	
}
