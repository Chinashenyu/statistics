package com.zdy.statistics.register;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RegisterReducer extends Reducer<Text, IntWritable, RegisterVo, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		RegisterVo rvo = new RegisterVo();
		
		int count = 0;
		for (IntWritable intWritable : values) {
			count += intWritable.get();
		}
		
		rvo.setNewDevice(count);
		rvo.setDate("2015-02-11");
		
		context.write(rvo, NullWritable.get());
	}

}
