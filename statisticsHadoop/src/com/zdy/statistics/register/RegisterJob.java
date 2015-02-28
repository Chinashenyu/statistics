package com.zdy.statistics.register;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

public class RegisterJob extends Configured implements Tool {

	
	/**
	 * @param args[]
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.mysql.jdbc.Driver");
		conf.set(DBConfiguration.URL_PROPERTY, "jdbc:mysql://192.168.8.124:3306/statisticsadmin");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "root");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "123456");
		
		conf.set("mapreduce.job.jar", "my.jar");
		
		DBConfiguration dbConf = new DBConfiguration(conf);
		dbConf.setOutputTableName("register_info");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(RegisterJob.class);
		
		job.setMapperClass(ResgisterMapper.class);
		job.setReducerClass(RegisterReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(RegisterVo.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(MyDBOutPutFormat.class);
		
		
		job.addCacheFile(new URI("hdfs://hadoop1:9000/test/register/invertedindex.txt"));
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop1:9000/test/register/testRegister.txt"));
		MyDBOutPutFormat.setOutput(job, "register_info", "new_device","day_time");
		
		return job.waitForCompletion(true)?0:1;

	}

}
