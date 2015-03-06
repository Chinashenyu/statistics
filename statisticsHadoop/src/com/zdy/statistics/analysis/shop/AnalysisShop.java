/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.zdy.statistics.analysis.shop;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.zdy.statistics.analysis.common.MongoDBConnector;

import java.net.UnknownHostException;
import java.util.List;

/**
 *
 * @author Administrator
 */
public class AnalysisShop {
    
    public int shopSell() throws UnknownHostException{
        
        DB db = MongoDBConnector.getMongoConnector(null, null, null);
        
        DBCollection collection = db.getCollection("server");
        
        BasicDBObject query = new BasicDBObject();
        query.put("type", "property");
        query.put("opera_time", new BasicDBObject("$gt","").append("$lt", ""));
        DBCursor cur = collection.find(query);
        
        return cur.count();
    }
    
}
