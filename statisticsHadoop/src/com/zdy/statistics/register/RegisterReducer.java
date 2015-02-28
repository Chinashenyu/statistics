package com.zdy.statistics.register;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zdy.statistics.util.DateTimeUtil;

public class RegisterReducer extends Reducer<Text, IntWritable, RegisterVo, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		RegisterVo rvo = new RegisterVo();
		
		int count = 0;
		for (IntWritable intWritable : values) {
			count += intWritable.get();
		}
		
		rvo.setNewDevice(count);
		rvo.setDate(DateTimeUtil.getyesterday());
		System.out.println("-------register reducer--------------------");
		context.write(rvo, NullWritable.get());
	}

}
