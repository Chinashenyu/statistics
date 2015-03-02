package com.zdy.statistics.register.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println("============ index combiner ================");
		long count = 0l;
		for (Text text : values) {
			count += Long.parseLong(text.toString());
		}
		
		String[] keys = key.toString().split("&");
		if(keys.length == 2){
			context.write(new Text(keys[0]), new Text(keys[1]+"&"+count));
		}
		
	}
	
}
