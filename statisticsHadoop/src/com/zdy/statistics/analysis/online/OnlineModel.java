package com.zdy.statistics.analysis.online;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OnlineModel implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2401456278021916291L;
	
	
	private Map<Integer,String> onlineMap = new HashMap<Integer,String>();

	public Map<Integer, String> getOnlineMap() {
		return onlineMap;
	}

	public void setOnlineMap(Map<Integer, String> onlineMap) {
		this.onlineMap = onlineMap;
	}
	
}
