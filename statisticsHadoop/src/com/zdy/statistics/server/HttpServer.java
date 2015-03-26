package com.zdy.statistics.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpServer {
	
	ServerSocket server = null;
	
	public void start() {

		ExecutorService executorService = Executors.newFixedThreadPool(10);
		
		try {
			server = new ServerSocket(9999);
			System.out.println("-----服务器 启动 -----");
			while (true) {
				Socket socket = server.accept();
				executorService.execute(new SocketProcessor(socket));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(server != null){
				try {
					server.close();
					System.out.println("-----服务器 关闭 -----");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static void main(String[] args) {
		HttpServer httpServer = new HttpServer();
		httpServer.start();
	}

}
