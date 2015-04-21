package com.zdy.statistics.analysis.online;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

public class OnlineMoniter {

	public void moniterOnlineMap() throws ParseException{
		
		Map<Integer, String> onlineMap = null;
		do {
			onlineMap = OnlineAnalysis.getOnlineMap();
		} while (onlineMap == null);
		
		SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date now = new Date(); 
		for (Entry<Integer, String> entry : onlineMap.entrySet()) {
			Integer userId = entry.getKey();
			String loginTime = entry.getValue();
			
			long timeDifference = (now.getTime() - dfs.parse(loginTime).getTime())/(1000*60*60);
			if(timeDifference > 12l){
				onlineMap.remove(userId);
			}
		}
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
