package com.zdy.statistics.register.invertedIndex.fileUtil;

import java.io.IOException;

public class PreparedFileUtil {

	public boolean mongoExport(String host,String port) throws IOException, InterruptedException{
		
		boolean result = false;
		
		String command = "mongoexport -h "+host+":"+port+" -d qipai -c server --cvs -f type,user_id,dev_id,register_time "
						 + "-q \"{'type':'register'}\" -o /home/hadoop/mongodata/register";
		Process process = Runtime.getRuntime().exec(command);
		if(process.waitFor() == 0){
			if(process.exitValue() == 0){
				result = true;
			}
		}
		
		return result;
	}
	
	public boolean uploadHDFS(){
		boolean result = false;
		
		
		
		return result;
	}
	
}
