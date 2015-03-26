package com.zdy.statistics.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class SocketProcessor implements Runnable {

	private Socket socket;
	private OutputStream outputStream;
	private BufferedReader reader;
	private PrintWriter out;
	
	public SocketProcessor() {
		
	}
	
	public SocketProcessor(Socket socket) {
		this.socket = socket;
//		System.out.println("***** 接收到 socket *****");
	}

	@Override
	public void run() {
		if(socket != null){
			try {
				System.out.println("***** 处理请求 ***** ： "+Thread.currentThread().getName());
				InputStream inputStream = socket.getInputStream();
				reader = new BufferedReader(new InputStreamReader(inputStream));
				String line = reader.readLine();
				System.out.println(line);
				
				String type = line.split(" ")[0];
				String resource = line.split(" ")[1];
				
				Class<?> class1 = Class.forName("com.zdy.statistics.analysis.");
				
				while((line = reader.readLine()) != null){
					System.out.println(line);
					if("GET".equals(type) && "".equals(line)){
						break;
					}
				}
				
//				int count = 0;
//				while(count == 0){
//					count = inputStream.available();
//				}
//				byte[] b = new byte[count];
//				inputStream.read(b);
//				String s = new String(b);
//				System.out.println(s);
				
				outputStream = socket.getOutputStream();
				out = new PrintWriter(outputStream);
				out.println("HTTP/1.0 200 OK");
//				out.println("Content-Type:text/html;charset=UTF-8");
				out.println();
				out.println("成功");
//				out.println(line);
				
//				reader.close();
				out.close();
				
				System.out.println("##### 完成请求 ##### ： "+Thread.currentThread().getName());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}finally{
				try {
//					reader.close();
//					out.close();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
