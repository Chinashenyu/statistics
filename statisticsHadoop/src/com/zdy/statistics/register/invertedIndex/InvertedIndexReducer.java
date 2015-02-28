package com.zdy.statistics.register.invertedIndex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		StringBuffer index = new StringBuffer("{");

		Iterator<Text> iterator = values.iterator();
		while(iterator.hasNext()){
			index.append(iterator.next());
			if(iterator.hasNext()){
				index.append(",");
			}
		}
		index.append("}");
		
		context.write(key, new Text(index.toString()));
		
	}
}
