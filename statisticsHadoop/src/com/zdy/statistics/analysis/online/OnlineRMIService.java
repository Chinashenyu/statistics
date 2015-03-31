package com.zdy.statistics.analysis.online;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface OnlineRMIService extends Remote {

	public Map<Integer,String> getOnlineMap()  throws RemoteException;

}