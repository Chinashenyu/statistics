/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zdy.statistics.analysis.common;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 *
 * @author Administrator
 */
public class MongoDBConnector {
    
	private static DB db;
	
    static {
    
    	Properties prop = new Properties();
    	try {
			prop.load(ClassLoader.getSystemResourceAsStream("common.properties"));
	    	String host = prop.getProperty("mongo.host");
	    	String port = prop.getProperty("mongo.port");
	    	String DBName = prop.getProperty("mongo.DBName");
	        
	    	MongoClient mongoClient = new MongoClient(host+":"+port);
			
			db = mongoClient.getDB(DBName);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static DB getDB(){
    	return db;
    }
}
