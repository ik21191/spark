/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mps.start;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mps.c5.processor.Processor;
import com.mps.commons.AppConfig;
import com.mps.enums.MongoConfig;
//import com.mps.utils.Constants;
import com.mps.utils.Methods;
import com.mps.utils.MyLogger;

/**
 *
 * @author kapil.verma
 */
public class Start {

	private List<Document> taskList = null;
	private int appSleepTime = 1000 * 60 * 2;
	private Methods methods = new Methods("ksv-spark");
    
    public Start() throws Exception{
    	
    }
    
	public void run() throws Exception {
		int taskProcessed = 0;
		int taskSkippedCompleted = 0;
		int taskStatus = 0; 
		String taskState = "";
		MyLogger.log("StartFilterProcessor : run() : Start");

		while(true) {
			taskStatus = 0;
			taskState = "";
			taskProcessed = 0;
			taskSkippedCompleted = 0;
			
			taskList = getTaskList();
			MyLogger.log("Total Task Found : " + taskList.size());
			// processing all the task defined in insight config
			
			// collection
			for(Document currentProcess : taskList) {
				AppConfig.initializeMongoConfig(currentProcess);
				try {
					taskStatus = Integer.parseInt(AppConfig.get("status"));
					taskState = AppConfig.get("state");

					if (taskStatus == 1 && taskState.equalsIgnoreCase("Active")) {
						taskProcessed++;
						MyLogger.log("***************");
						MyLogger.log("Task InProgress : " + currentProcess.getObjectId("_id").toString() + " : " + taskList.size() + "/" + taskSkippedCompleted + "/" + taskProcessed);

						AppConfig.setCurrentProcess(currentProcess);
						Processor processor = new Processor();
						processor.run();
						MyLogger.log("Task Processed : Total/Skipped/Processd : " + taskList.size() + "/" + taskSkippedCompleted + "/" + taskProcessed + " : SUCCESS");
					} else {
						taskSkippedCompleted++;
					}
					
				} catch (Exception e) {
					MyLogger.log("Task Exception :  Total/Skipped/Processd : " + taskList.size() + "/" + taskSkippedCompleted + "/" + taskProcessed + " : " + e.toString() + " : FAIL");
				}
			}

			MyLogger.log("Total Task Processed :  Total/Skipped/Processd : " + taskList.size() + "/"+ taskSkippedCompleted + "/" + taskProcessed);
			MyLogger.log("Sleeping app for " + (appSleepTime / 1000) + " secs.");
			Thread.sleep(appSleepTime);
		}

	}
    
	private List<Document> getTaskList() throws Exception {
	    	
	    	MongoClient client = null;
	    	try {
	    		String mongoClient = AppConfig.get(MongoConfig.MONGO_HOST.getValue());
	    		String mongoDB = AppConfig.get(MongoConfig.MONGO_DATABASE.getValue());
	    		String mongoCollection = AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue());
	    	    client = new MongoClient(mongoClient);
	    		MongoDatabase database = client.getDatabase(mongoDB);
	    		
	    		//Creating where clause
	    		BasicDBObject andQuery = new BasicDBObject();
	    	    List<BasicDBObject> whereConditionList = new ArrayList<>();
	    	    whereConditionList.add(new BasicDBObject("status", 1));
	    	    whereConditionList.add(new BasicDBObject("state", "Active"));
	    	    andQuery.put("$and", whereConditionList);
	    		
	    		MongoCollection<Document> collection = database.getCollection(mongoCollection);
	    		//Fetching data from Mongo with where clause
	    		List<Document> list = (List<Document>) collection.find(andQuery).into(new ArrayList<Document>()); 
	    		return list;
	    		
	    	}catch(Exception e) {
	    		MyLogger.error("Start: getTaskList(): EXCEPTION: " + e);
	    	} finally {
	    		if(client != null) {
	    			client.close();
	    		}
	    	}
	    	return Collections.emptyList();
	    }  
}
