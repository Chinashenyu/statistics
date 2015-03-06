package com.zdy.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class MainRun {

	
	
	public MainRun(){
		
	}
	
	public void init(){
		Properties properties = new Properties();
		FileInputStream input = null;
		try {
			input = new FileInputStream(new File("common.properties"));
			properties.load(input);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
	}

}
