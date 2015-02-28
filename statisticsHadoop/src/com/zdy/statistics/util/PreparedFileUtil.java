package com.zdy.statistics.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PreparedFileUtil {

	public boolean mongoExport(String host,String port,String DB,String collection,String title,String query,String outputPath){
		
		boolean result = false;
		
		StringBuffer command = new StringBuffer("mongoexport");
		
		if(host == null || "".equals(host)){
			host = "127.0.0.1";
		}
		command.append(" -h "+host);
		if(port == null || "".equals(port)){
			port = "27017";
		}
		command.append(":"+port);
		if(DB != null && !"".equals(DB)){
			command.append(" -d "+DB);
		}else{
			return false;
		}
		if(collection != null && !"".equals(collection)){
			command.append(" -c "+collection);
		}else{
			return false;
		}
		if(title != null && !"".equals(title)){
			command.append(" --csv -f "+title);
		}else{
			return false;
		}
		if(query != null && !"".equals(query)){
			command.append(" -q "+query);
		}
		if(outputPath != null && !"".equals(outputPath)){
			command.append(" -o "+outputPath);
		}else{
			return false;
		}
		
		Process process = null;
		try {
			process = Runtime.getRuntime().exec("mongoexport -h 192.168.8.124:27017 -d qipai -c server --csv -f type,user_id,dev_id,regisrer_time -q {'type':'register'} -o /home/hadoop/mongodata/register/registerBackup-2015-02-27.csv");
			process.getInputStream().read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			
			System.out.println(process.waitFor());
			if(process.waitFor() == 0){
				if(process.exitValue() == 0){
					result = true;
				}
			}else{
				System.out.println("command exec error !");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public boolean loadMongoDBToHDFS(String HDFSOutputPath,String host,String port,String DB,String collection,String tile,String query,String mongoOutputPath) throws IOException, InterruptedException{
		boolean result = false;
		
		if(mongoExport(host, port, DB, collection, tile, query, mongoOutputPath)){
			
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			
			Path inputPath = new Path(mongoOutputPath);
			Path outputPath = new Path(HDFSOutputPath);
			
			hdfs.copyFromLocalFile(inputPath, outputPath);
			result = true;
		}
		
		return result;
	}
}
