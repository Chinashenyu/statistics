package com.zdy.statistics.register.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InversedIndexMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private Text outKey;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] values = line.split(",");
		
		if(values.length == 4){
			if(values[0].equals("register")){
				//context.write(, value);
			}
		}
		
	}
	
}
