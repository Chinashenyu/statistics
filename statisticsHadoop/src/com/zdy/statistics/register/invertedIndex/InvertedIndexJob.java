package com.zdy.statistics.register.invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.zdy.statistics.register.invertedIndex.InvertedIndexCombiner;
import com.zdy.statistics.register.invertedIndex.InvertedIndexMapper;
import com.zdy.statistics.register.invertedIndex.InvertedIndexReducer;

public class InvertedIndexJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.job.jar", "my.jar");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(InvertedIndexJob.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
		Path path = new Path("hdfs://hadoop1:9000/statistics/qipai/register/invertedindex");
		if(fs.exists(path)){
			fs.delete(path,true);
		}
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop1:9000/statistics/qipai/register/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop1:9000/statistics/qipai/register/invertedindex"));
		
		return job.waitForCompletion(true)?0:1;
	}
	
//	public static void main(String[] args) throws Exception {
//		int result = ToolRunner.run(new Configuration(), new InversedIndexJob(), args);
//	}
}
