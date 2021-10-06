package com.mps.c5.stats;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mps.mongo.MongoDBActions;
import com.mps.utils.Constants;
import com.mps.utils.MyLogger;

public class InstitutionIPStats implements Serializable{
	private static final long serialVersionUID = 1L;
	JavaSparkContext jsc = null;
	
	public InstitutionIPStats(JavaSparkContext jsc) {
		this.jsc = jsc;
	}
	
	public boolean dataInsertMonthlyHorizontal(String fromDatabase, String fromCollection, String fromURI, String toDatabase, String toCollection, String toURI) throws Exception {
		boolean status = false;
		HashMap<String, String> readOverrides = new HashMap<>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {
    		//************************ PAGE TYPE PATTERN LOGIC *************************
    		//Get all page page patterns from log_page_types_memory table
    		MyLogger.log("InstitutionIPStats : dataInsertMonthlyHorizontal : Start");
    		
    		readOverrides.put(Constants.DATABASE, fromDatabase);
            readOverrides.put(Constants.COLLECTION, fromCollection);
            readOverrides.put("uri", fromURI);
            
            MyLogger.log("InstitutionIPStats : dataInsertMonthlyHorizontal : Loading readOverrides : " + readOverrides + " :  Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            
          //If counter exists then proceed else return error
			if(!MongoDBActions.isCollectionExists(fromURI, fromDatabase, fromCollection)) {
				MyLogger.error("InstitutionIPStats: dataInsertMonthlyHorizontal: Collection does not exists, skipping further processing");
				return false;
			}
           
            iTracker = 2.0;
            Dataset<Row> dataSet = MongoSpark.load(jsc, readConfig).toDF();
            Dataset<Row> out = dataSet.filter(dataSet.col(Constants.PAGE_METRIC_ID).equalTo(82).or(dataSet.col(Constants.PAGE_METRIC_ID).equalTo(83)).or(dataSet.col(Constants.PAGE_METRIC_ID).equalTo(136)).or(dataSet.col(Constants.PAGE_METRIC_ID).equalTo(137))).toDF();
            MyLogger.log("InstitutionIPStats : dataInsertMonthlyHorizontal : Loading readOverrides : " + readOverrides + " :  End");
            iTracker = 3.0;
            
            String collectionYrMonthDay[] = fromCollection.split("_");
            String yearMonthDay = "";
            String year = "";
            String month = "";
            int day = 0;
            if(collectionYrMonthDay.length == 2){
            	yearMonthDay = collectionYrMonthDay[1];
	            year = yearMonthDay.substring(0, 4);
	            month = yearMonthDay.substring(4,6);
	            day = Integer.parseInt(yearMonthDay.substring(6));
            }
            iTracker = 3.0;
            MyLogger.log("InstitutionIPStats : dataInsertMonthlyHorizontal : dataSet.groupBy() : Start");
            Dataset<Row> output = out.groupBy("institution_id", "ip_address")
            		.count().withColumnRenamed("count", "d_" + day) 
            		.withColumn("day", functions.lit(day))
            		.withColumn("year", functions.lit("astm"))
            		.withColumn("year", functions.lit(year))
            		.withColumn("month", functions.lit(month))
            		.withColumn("_id", functions.lower(functions.concat(functions.col("institution_id"), functions.lit("~#~"), 
            				functions.col("ip_address"), functions.lit("~#~"), 
            				functions.lit(year), functions.lit("~#~"),
            				functions.lit(month))));
       
            MyLogger.log("InstitutionIPStats : dataInsertMonthlyHorizontal : dataSet.groupBy() : End");
            iTracker = 5.0;
           
            writeOverrides.put(Constants.DATABASE, toDatabase);
            writeOverrides.put(Constants.COLLECTION, toCollection);
            writeOverrides.put("uri", toURI);
            writeOverrides.put("replaceDocument", "false");
            
            MyLogger.log("InstitutionIPStats : dataInsertMonthlyHorizontal : Saving to : "+ writeOverrides.toString() + " : Start");
            iTracker = 6.0;
            
            MongoSpark.save(output.write().options(writeOverrides).mode("append"));
            //MongoSpark.save(output.write().option("database", writeDatabase).option("collection", writeCollection).option("uri", uri).option("replaceDocument", "false").mode("append"));
            MyLogger.log("InstitutionIPStats : dataInsertMonthlyHorizontal : Saving to : "+ writeOverrides.toString() + " : End");            
          
            iTracker = 7.0;
            status = true;
            
            MyLogger.log("InstitutionIPStats : dataInsertMonthlyHorizontal : End");

    	} catch (Exception e) {
            MyLogger.error("InstitutionIPStats : dataInsertMonthlyHorizontal : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}
	
	//This method is not used as of now as there is no need of ip institution stats year wise 
	public boolean dataInsertYearlyHorizontal(String fromDatabase, String fromCollection, String fromURI, String toDatabase, String toCollection, String toURI) throws Exception {
		boolean status = false;
		HashMap<String, String> readOverrides = new HashMap<>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {
    		//************************ PAGE TYPE PATTERN LOGIC *************************
    		//Get all page page patterns from log_page_types_memory table
    		MyLogger.log("InstitutionIPStats : dataInsertYearlyHorizontal : Start");
    		
    		readOverrides.put(Constants.DATABASE, fromDatabase);
            readOverrides.put(Constants.COLLECTION, fromCollection);
            readOverrides.put("uri", fromURI);
            
          //If counter exists then proceed else return error
			if(!MongoDBActions.isCollectionExists(fromURI, fromDatabase, fromCollection)) {
				MyLogger.error("InstitutionIPStats: dataInsertYearlyHorizontal: Collection does not exists, skipping further processing");
				return false;
			}
            
            MyLogger.log("InstitutionIPStats : dataInsertYearlyHorizontal : Loading readOverrides : " + readOverrides + " :  Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
           
            iTracker = 2.0;
            Dataset<Row> dataSet = MongoSpark.load(jsc, readConfig).toDF();
            Dataset<Row> out = dataSet.filter(dataSet.col(Constants.PAGE_METRIC_ID).equalTo(82).or(dataSet.col(Constants.PAGE_METRIC_ID).equalTo(83)).or(dataSet.col(Constants.PAGE_METRIC_ID).equalTo(136)).or(dataSet.col(Constants.PAGE_METRIC_ID).equalTo(137))).toDF();
            MyLogger.log("InstitutionIPStats : dataInsertYearlyHorizontal : Loading readOverrides : " + readOverrides + " :  End");
            iTracker = 3.0;
            
            String collectionYrMonthDay[] = fromCollection.split("_");
            String yearMonthDay = "";
            String year = "";
            String month = "";
            int day = 0;
            if(collectionYrMonthDay.length == 2){
            	yearMonthDay = collectionYrMonthDay[1];
	            year = yearMonthDay.substring(0, 4);
	            month = yearMonthDay.substring(4,6);
	            day = Integer.parseInt(yearMonthDay.substring(6));
            }
            iTracker = 3.0;
            MyLogger.log("InstitutionIPStats : dataInsertYearlyHorizontal : dataSet.groupBy() : Start");
            Dataset<Row> output = out.groupBy("institution_id", "ip_address")
            		.count().withColumnRenamed("count", "d_" + day) 
            		.withColumn("day", functions.lit(day))
            		.withColumn("year", functions.lit("astm"))
            		.withColumn("year", functions.lit(year))
            		.withColumn("month", functions.lit(month))
            		.withColumn("_id", functions.lower(functions.concat(functions.col("institution_id"), functions.lit("~#~"), functions.col("ip_address"), functions.lit("~#~"), functions.lit(year), functions.lit("~#~"),functions.lit(month))));
       
            MyLogger.log("InstitutionIPStats : dataInsertYearlyHorizontal : dataSet.groupBy() : End");
            iTracker = 5.0;
           
            writeOverrides.put(Constants.DATABASE, toDatabase);
            writeOverrides.put(Constants.COLLECTION, toCollection);
            writeOverrides.put("uri", toURI);
            writeOverrides.put("replaceDocument", "false");
            
            MyLogger.log("InstitutionIPStats : dataInsertYearlyHorizontal : Saving to : "+ writeOverrides.toString() + " : Start");
            iTracker = 6.0;
            
            MongoSpark.save(output.write().options(writeOverrides).mode("append"));
            
            //MongoSpark.save(output.write().option("database", writeDatabase).option("collection", writeCollection).option("uri", uri).option("replaceDocument", "false").mode("append"));
            
            MyLogger.log("InstitutionIPStats : dataInsertYearlyHorizontal : Saving to : "+ writeOverrides.toString() + " : End");            
          
            iTracker = 7.0;
            status = true;
            
            MyLogger.log("InstitutionIPStats : dataInsertYearlyHorizontal : End");

    	} catch (Exception e) {
            MyLogger.error("InstitutionIPStats : dataInsertYearlyHorizontal : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}	
}
