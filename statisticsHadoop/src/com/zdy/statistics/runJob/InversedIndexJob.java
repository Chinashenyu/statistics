package com.zdy.statistics.runJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.zdy.statistics.register.invertedIndex.InversedIndexCombiner;
import com.zdy.statistics.register.invertedIndex.InversedIndexMapper;
import com.zdy.statistics.register.invertedIndex.InversedIndexReducer;

public class InversedIndexJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.jar", "my.jar");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(InversedIndexJob.class);
		
		job.setMapperClass(InversedIndexMapper.class);
		job.setCombinerClass(InversedIndexCombiner.class);
		job.setReducerClass(InversedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop1:9000/test/register/testRegister.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/test/register/inversed"));
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new InversedIndexJob(), args);
	}
}
