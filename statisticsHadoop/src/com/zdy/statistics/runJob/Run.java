package com.zdy.statistics.runJob;

import java.io.IOException;

import org.apache.hadoop.util.ToolRunner;

import com.zdy.statistics.register.RegisterJob;
import com.zdy.statistics.register.invertedIndex.InvertedIndexJob;

public class Run {

	public void mongoExporty(){
		Runtime run = Runtime.getRuntime();
		
		try {
			Process pro = run.exec("mongoexport -d qipai -c server --csv -f type,user_id,login_time -q \"{'user_id':'100000'}\" -o e:\\serverBackup.dat");
			
			//pro.getInputStream();
			if(pro.waitFor() == 0){//0表示 正常终止
				System.out.println("PROCESS IS NOT WAITING");
				if(pro.exitValue() == 0){//p.exitValue()==0表示正常结束，1：非正常结束  
					System.out.println("success");
					
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	
	public static void main(String[] args) {
		try {
			int indexResult = ToolRunner.run(new InvertedIndexJob(), args);
			if(indexResult == 1){
				int registerResult = ToolRunner.run(new RegisterJob(), args);
				
				System.exit(registerResult);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
