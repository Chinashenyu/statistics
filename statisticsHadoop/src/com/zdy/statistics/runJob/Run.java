package com.zdy.statistics.runJob;

import org.apache.hadoop.util.ToolRunner;

import com.zdy.statistics.register.RegisterJob;
import com.zdy.statistics.register.invertedIndex.InvertedIndexJob;
import com.zdy.statistics.util.DateTimeUtil;
import com.zdy.statistics.util.PreparedFileUtil;

public class Run {

	public static void main(String[] args) {
		try {
			
			String host = "192.168.8.124";
			String port = "27017";
			String DB = "qipai";
			String collection = "server";
			String tile = "type,user_id,dev_id,regisrer_time";
			String query = "\"{'type':'register'}\"";
			String mongoOutputPath = "/home/hadoop/mongodata/register/registerBackup-"+DateTimeUtil.getyesterday()+".csv";
			
			String HDFSOutputPath = "hdfs://hadoop1:9000/statistics/qipai/register/registerBackup-"+DateTimeUtil.getyesterday()+".csv";
			
			PreparedFileUtil pfu = new PreparedFileUtil();
			boolean result = pfu.loadMongoDBToHDFS(HDFSOutputPath, host, port, DB, collection, tile, query, mongoOutputPath);
			String[] args1 = {"",""};
			args1[0] = HDFSOutputPath;
			args1[1] = "";
			if(result){
				int registerResult = ToolRunner.run(new RegisterJob(), args1);
				if(registerResult == 1){
					int indexResult = ToolRunner.run(new InvertedIndexJob(), args1);
					System.exit(indexResult);
				}
			}else{
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
