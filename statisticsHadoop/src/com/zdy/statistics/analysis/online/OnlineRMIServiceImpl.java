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
			OnlineRMIService onlineRMIService = new OnlineRMIServiceImpl();
			
			LocateRegistry.createRegistry(8000);
			Naming.rebind("rmi://127.0.0.1:8000/OnlineRMIService", onlineRMIService);
			System.out.println("rmi is starting ... ");
			logger.info("RMI 服务 开启成功");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
