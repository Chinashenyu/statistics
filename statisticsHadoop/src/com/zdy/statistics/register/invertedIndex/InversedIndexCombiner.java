package com.zdy.statistics.register.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InversedIndexCombiner extends Reducer<Text, LongWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		long count = 0l;
		for (LongWritable longWritable : values) {
			count += longWritable.get();
		}
		
		String[] keys = key.toString().split(":");
		if(keys.length == 2){
			context.write(new Text(keys[0]), new Text(keys[1]+":"+count));
		}
		
	}
	
}
