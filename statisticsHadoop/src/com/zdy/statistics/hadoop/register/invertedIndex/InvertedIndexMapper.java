package com.zdy.statistics.hadoop.register.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outKey = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] values = line.split(",");
		System.out.println("============ index mapper ================");
		if(values.length == 4){
			if(values[0].equals("\"register\"")){
				
				outKey.set(values[2]+"&"+values[3]);
				
				context.write(outKey, new Text("1"));
			}
		}
		
	}
	
}
