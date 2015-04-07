package com.zdy.statistics.analysis.online;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

import org.apache.log4j.Logger;

public class OnlineRMIServiceImpl extends UnicastRemoteObject implements OnlineRMIService {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6434873311308312677L;

	private static Logger logger = Logger.getLogger(OnlineRMIServiceImpl.class);

	protected OnlineRMIServiceImpl() throws RemoteException {
		super();
	}

	@Override
	public Map<Integer,String> getOnlineMap() {
	
		Map<Integer, String> onlineMap = OnlineAnalysis.getOnlineMap();
		
		return onlineMap;
	}
	
	
	public static void startRMIService() {
		try {
			System.setProperty("java.rmi.server.hostname","120.55.99.53");
			OnlineRMIService onlineRMIService = new OnlineRMIServiceImpl();
			
			LocateRegistry.createRegistry(8989);
			Naming.rebind("rmi://120.55.99.53:8989/OnlineRMIService", onlineRMIService);
			System.out.println("rmi is starting ... ");
			logger.info("RMI 服务 开启成功 端口号：8989");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
