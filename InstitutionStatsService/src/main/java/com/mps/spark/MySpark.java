package com.mps.spark;

import java.io.File;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.mps.utils.Methods;
import com.mps.utils.MyLogger;


public class MySpark {

	private Methods methods = new Methods();
	
	private String sparkConfigFilePath = "";
	private SparkSession session;
	
	//MySpark Class constructor
	public MySpark() throws Exception{
		HashMap<String, String> sparkConfig = new HashMap<>();
		try {
			MyLogger.log("MySpark : Start");
			
			sparkConfigFilePath = methods.getClassPath(this) + File.separator + "config" + File.separator + "spark.mps";
			File sparkFile = new File(sparkConfigFilePath);
			
			if(!sparkFile.exists()){
				throw new Exception("MySpark : sparkConfigFilePath : Does Not Exists : " + sparkConfigFilePath);
			}
			MyLogger.log("MySpark : sparkConfigFilePath : " + sparkConfigFilePath);
			
			//
			sparkConfig = methods.readPropertyFile(sparkConfigFilePath);
			
			MyLogger.log("MySpark : Reading Spark config: Start ");
			MyLogger.log("spark.master" + "=" + sparkConfig.get("spark.master"));
			MyLogger.log("spark.appname" + "=" + sparkConfig.get("spark.appname"));
			
			String sparkMaster = sparkConfig.get("spark.master");
			String sparkAppName = sparkConfig.get("spark.appname");
			Builder builder = SparkSession.builder();
			builder.master(sparkMaster).appName(sparkAppName);
			
			for(Entry<String, String> keyValuePair : sparkConfig.entrySet()) {
				String key = keyValuePair.getKey();
				String value = keyValuePair.getValue();
				
				if(!(key.equalsIgnoreCase("spark.master") || key.equalsIgnoreCase("spark.appname"))) {
					builder = builder.config(key, value);
					MyLogger.log(key + "=" + value);
				}
			}
			MyLogger.log("MySpark : Reading Spark config: End ");
			session = builder.getOrCreate();
			MyLogger.log(session.sparkContext().toString());
			MyLogger.log("SparkSession : Generation : Done");
		} catch (Exception e) {
			MyLogger.log("MySpark : Exception : " + e.toString());
			throw e;
		}
	}
	
	//returning JavaSparkContext
    public JavaSparkContext getJavaSparkContext(){
    	JavaSparkContext jsc = null;
    	double iTracker = 0.0;
    	try{
	        //
	        MyLogger.log("JavaSparkContext Generation : Start");
	    	jsc = new JavaSparkContext(session.sparkContext());	    	
	    	MyLogger.log("JavaSparkContext Generation : Done");
    	}
    	catch(Exception e){
    		MyLogger.exception("getJavaSparkContext : " + iTracker + " : " +  e.toString());
    	} 
        return jsc;
    }
	
	//returning SparkSession
    public SparkSession getSparkSession(){
        return this.session;
    }
	
	
	
}
