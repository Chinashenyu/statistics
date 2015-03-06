/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zdy.statistics.analysis.common;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;

/**
 *
 * @author Administrator
 */
public class MongoDBConnector {
    
    public static DB getMongoConnector(String host,String port,String DBName) throws UnknownHostException{
    
        MongoClient mongoClient = new MongoClient(host+port);
        
        DB db = mongoClient.getDB(DBName);
        
        return db;
    }
    
}
