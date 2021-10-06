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
import com.mps.utils.Constants;
import com.mps.utils.MyLogger;

public class InstitutionStatsInsertion implements Serializable{
	private static final long serialVersionUID = 1L;
	JavaSparkContext jsc = null;
	
	public InstitutionStatsInsertion(JavaSparkContext jsc) {
		this.jsc = jsc;
	}
	
	public boolean verticalStatsDailyToDaily(String sourceDatabase, String sourceCollection, String sourceURI, String destinationDatabase, String destinationCollection, String destinationURI) throws Exception {
		boolean status = false;
		HashMap<String, String> readOverrides = new HashMap<>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {

    		MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : Start");
    		
    		readOverrides.put(Constants.DATABASE, sourceDatabase);
            readOverrides.put(Constants.COLLECTION, sourceCollection);
            readOverrides.put(Constants.MONGO_HOST_URI, sourceURI);
            iTracker = 1.0;
            
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
           
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : Processing : " + readOverrides.toString());
            
            iTracker = 2.0;
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : MongoSpark.load : dataSet : Start");
            Dataset<Row> dataSet = MongoSpark.load(jsc,readConfig).toDF();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : MongoSpark.load : dataSet : Done");
            
            iTracker = 3.0;
            
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : dataSet.groupBy() : Start");
            Dataset<Row> output = dataSet.groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.YEAR, Constants.MONTH, Constants.DAY)
					.count();
			
            output.cache();
            
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : dataSet.groupBy() : End");
            iTracker = 5.0;
           
            String writeDatabase = destinationDatabase;
            String writeCollection = destinationCollection;
            writeOverrides.put(Constants.DATABASE, writeDatabase);
            writeOverrides.put(Constants.COLLECTION, writeCollection);
            writeOverrides.put(Constants.MONGO_HOST_URI, destinationURI);
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : Destination WriteConfig : "+ writeOverrides.toString() + " Start");
            iTracker = 6.0;
            
            output.write().format("com.mongodb.spark.sql").mode("overwrite").options(writeOverrides).save();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : Destination WriteConfig : "+ writeOverrides.toString() + " End");
            
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToDaily : End");
            status = true;
            
		} catch (Exception e) {
			MyLogger.error("InstitutionStatsInsertion : verticalStatsDailyToDaily : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}
	
	public boolean verticalStatsDailyToMonthly(String sourceDatabase, String sourceCollection, String sourceURI, String destinationDatabase, String destinationCollection, String destinationURI) throws Exception{
		boolean status = false;
		
    	HashMap<String, String> readOverrides = new HashMap<String, String>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {
    
    		MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToMonthly : Start");
    		
    		readOverrides.put(Constants.DATABASE, sourceDatabase);
            readOverrides.put(Constants.COLLECTION, sourceCollection);
            readOverrides.put(Constants.MONGO_HOST_URI, sourceURI);
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToMonthly : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
        
            iTracker = 2.0;
            Dataset<Row> dataSet = MongoSpark.load(jsc,readConfig).toDF();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToMonthly : Load Processing : " + readOverrides + " : End");
            
            iTracker = 3.0;
            
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToMonthly : dataSet.groupBy() : Start");
            Dataset<Row> output = dataSet.groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.YEAR, Constants.MONTH, Constants.DAY)
					.count();

            //output.cache();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToMonthly : dataSet.groupBy() : End");
            
            iTracker = 5.0;
           
            String writeDatabase = destinationDatabase;
            String writeCollection = destinationCollection;
            writeOverrides.put(Constants.DATABASE, writeDatabase);
            writeOverrides.put(Constants.COLLECTION, writeCollection);
            writeOverrides.put("uri", destinationURI);
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToMonthly : Destination WriteConfig : "+ writeOverrides.toString() + " Start");
            iTracker = 6.0;
            
            output.write().format("com.mongodb.spark.sql").mode("append").options(writeOverrides).save();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToMonthly : Destination WriteConfig : "+ writeOverrides.toString() + " End");
            
            iTracker = 7.0;
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToMonthly :  End");
            status = true;
            
            //******************************************************************************
		} catch (Exception e) {
			MyLogger.error("InstitutionStatsInsertion : verticalStatsDailyToMonthly : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}
	
	public boolean verticalStatsDailyToYearly(String sourceDatabase, String sourceCollection, String sourceURI, String destinationDatabase, String destinationCollection, String destinationURI) throws Exception{
		boolean status = false;
		HashMap<String, String> readOverrides = new HashMap<String, String>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {
    		readOverrides.put(Constants.DATABASE, sourceDatabase);
            readOverrides.put(Constants.COLLECTION, sourceCollection);
            readOverrides.put(Constants.MONGO_HOST_URI, sourceURI);
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToYearly : Load processing : " + readOverrides.toString() + ":  Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
           
            iTracker = 2.0;
            Dataset<Row> dataSet = MongoSpark.load(jsc,readConfig).toDF();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToYearly : Load processing : " + readOverrides.toString() + " : End");
            
            iTracker = 3.0;
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToYearly : dataSet.groupBy : Start");
            Dataset<Row> output = dataSet.groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.DAY)
					.count();
	        
            //output.cache();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToYearly : dataSet.groupBy : End");
            
            iTracker = 5.0;
           
            String writeDatabase = destinationDatabase;
            String writeCollection = destinationCollection;
            writeOverrides.put(Constants.DATABASE, writeDatabase);
            writeOverrides.put(Constants.COLLECTION, writeCollection);
            writeOverrides.put(Constants.MONGO_HOST_URI, destinationURI);

            iTracker = 6.0;
            
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToYearly : Processing DataSet.write() for : " + writeOverrides.toString() + " : Start");
            output.write().format("com.mongodb.spark.sql").mode("append").options(writeOverrides).save();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToYearly : Processing DataSet.write() for : " + writeOverrides.toString() + " : End");

            iTracker = 8.0;
        
            MyLogger.log("InstitutionStatsInsertion : verticalStatsDailyToYearly : End");
            status = true;
            
		} catch (Exception e) {
			MyLogger.error("InstitutionStatsInsertion : verticalStatsDailyToYearly : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}
	
	public boolean verticalStatsMonthlyToDaily(String sourceDatabase, String sourceCollection, String sourceURI, String destinationDatabase, String destinationCollection, String destinationURI) throws Exception{
		boolean status = false;
		
    	HashMap<String, String> readOverrides = new HashMap<String, String>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {
    
    		MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToDaily : Start");
    		
    		readOverrides.put(Constants.DATABASE, sourceDatabase);
            readOverrides.put(Constants.COLLECTION, sourceCollection);
            readOverrides.put(Constants.MONGO_HOST_URI, sourceURI);
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToDaily : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
        
            iTracker = 2.0;
            Dataset<Row> dataSet = MongoSpark.load(jsc,readConfig).toDF();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToDaily : Load Processing : " + readOverrides + " : End");
            
            iTracker = 3.0;
            
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToDaily : dataSet.groupBy() : Start");
            Dataset<Row> output = dataSet.groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.YEAR, Constants.MONTH, Constants.DAY)
					.count();

            //output.cache();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToDaily : dataSet.groupBy() : End");
            
            iTracker = 5.0;
           
            String writeDatabase = destinationDatabase;
            String writeCollection = destinationCollection;
            writeOverrides.put(Constants.DATABASE, writeDatabase);
            writeOverrides.put(Constants.COLLECTION, writeCollection);
            writeOverrides.put("uri", destinationURI);
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToDaily : Destination WriteConfig : "+ writeOverrides.toString() + " Start");
            iTracker = 6.0;
            
            output.write().format("com.mongodb.spark.sql").mode("append").options(writeOverrides).save();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToDaily : Destination WriteConfig : "+ writeOverrides.toString() + " End");
            
            iTracker = 7.0;
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToDaily :  End");
            status = true;
            
            //******************************************************************************
		} catch (Exception e) {
			MyLogger.error("InstitutionStatsInsertion : verticalStatsMonthlyToDaily : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}
	
	
	
	public boolean verticalStatsMonthlyToMonthly(String sourceDatabase, String sourceCollection, String sourceURI, String destinationDatabase, String destinationCollection, String destinationURI) throws Exception{
		boolean status = false;
		
    	HashMap<String, String> readOverrides = new HashMap<>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {
    
    		MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly : Start");
    		
    		readOverrides.put(Constants.DATABASE, sourceDatabase);
            readOverrides.put(Constants.COLLECTION, sourceCollection);
            readOverrides.put(Constants.MONGO_HOST_URI, sourceURI);
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
        
            iTracker = 2.0;
            Dataset<Row> dataSet = MongoSpark.load(jsc,readConfig).toDF();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly : Load Processing : " + readOverrides + " : End");
            
            iTracker = 3.0;
            
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly : dataSet.groupBy() : Start");
            Dataset<Row> output = dataSet.groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.YEAR, Constants.MONTH)
					.count();

            //output.cache();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly : dataSet.groupBy() : End");
            
            iTracker = 5.0;
           
            String writeDatabase = destinationDatabase;
            String writeCollection = destinationCollection;
            writeOverrides.put(Constants.DATABASE, writeDatabase);
            writeOverrides.put(Constants.COLLECTION, writeCollection);
            writeOverrides.put(Constants.MONGO_HOST_URI, destinationURI);
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly : Destination WriteConfig : "+ writeOverrides.toString() + " Start");
            iTracker = 6.0;
            
            output.write().format("com.mongodb.spark.sql").mode("append").options(writeOverrides).save();
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly : Destination WriteConfig : "+ writeOverrides.toString() + " End");
            
            iTracker = 7.0;
            MyLogger.log("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly :  End");
            status = true;
            
		} catch (Exception e) {
			MyLogger.error("InstitutionStatsInsertion : verticalStatsMonthlyToMonthly : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}
	
	
	
	public boolean insertInstStatsMonthlyHorizontal(String sourceDatabase, String sourceCollection, String sourceURI, String destinationDatabase, String destinationCollection, String destinationURI) throws Exception{
		boolean status = false;
		HashMap<String, String> readOverrides = new HashMap<String, String>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {
    		MyLogger.log("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal :  Start");
    		
            readOverrides.put(Constants.DATABASE, sourceDatabase);
            readOverrides.put(Constants.COLLECTION, sourceCollection);
            readOverrides.put(Constants.MONGO_HOST_URI, sourceURI);
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal : Load processing : " + readOverrides.toString() + " :  Start");
            iTracker = 1.0;
            
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
           
            iTracker = 2.0;
            
            Dataset<Row> dataSet = MongoSpark.load(jsc,readConfig).toDF();
            
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal : Load processing : " + readOverrides.toString() + " :  End");
            
            iTracker = 3.0;
            MyLogger.log("dataSet.groupBy() : Start");
            String collectionYrMonthDay[] = sourceCollection.split("_");
            String yearMonthDay = "";
            String year="";
            String month="";
            int day=0;
            if(collectionYrMonthDay.length == 2){
            	yearMonthDay = collectionYrMonthDay[1];
	            year = yearMonthDay.substring(0, 4);
	            month = yearMonthDay.substring(4,6);
	            day = Integer.parseInt(yearMonthDay.substring(6));
            }
            
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal : dataSet.groupBy : Start");
            Dataset<Row> output = dataSet.groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD)
            		.count().withColumnRenamed("count", "d_"+day)
            		.withColumn("year", functions.lit(year))
                    .withColumn("month", functions.lit(month))
                    .withColumn("day", functions.lit(day))
                    .withColumn("_id", functions.lower(functions.concat(functions.col("institution_id"), functions.lit("~~"), 
                    		functions.col(Constants.JOURNAL_ID), functions.lit("~~"), 
                    		(functions.when(functions.col(Constants.ARTICLE_ID).contains("%").contains("{"), "invalidai").otherwise(functions.col(Constants.ARTICLE_ID))), functions.lit("~~"), 
                    		functions.col(Constants.PAGE_METRIC_ID), functions.lit("~~"), 
                    		functions.col(Constants.ACCESS_METHOD), functions.lit("~~"), 
                    		functions.lit(year),functions.lit("~~"),
                    		functions.lit(month))));
         
            //output.cache();
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal : dataSet.groupBy : End");
            
            iTracker = 5.0;
           
            String writeDatabase = destinationDatabase;
            String writeCollection = destinationCollection;
            writeOverrides.put(Constants.DATABASE, writeDatabase);
            writeOverrides.put(Constants.COLLECTION, writeCollection);
            writeOverrides.put(Constants.MONGO_HOST_URI, destinationURI);
            writeOverrides.put("replaceDocument", "false");
            
            MyLogger.log("Destination WriteConfig : insertInstStatsMonthlyHorizontal : "+ writeOverrides.toString());
            iTracker = 6.0;
            
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal : Processing DataSet.write() for : " + writeOverrides.toString() + " : Start");
            output.write().format("com.mongodb.spark.sql").mode("append").options(writeOverrides).save();
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal : Processing DataSet.write() for : " + writeOverrides.toString() + " : End");
            
            iTracker = 8.0;
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal :  End");
            status = true;

    	} catch (Exception e) {
			MyLogger.error("InstitutionStatsInsertion : insertInstStatsMonthlyHorizontal : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}
	
	public boolean insertInstStatsYearlyHorizontal(String sourceDatabase, String sourceCollection, String sourceURI, String destinationDatabase, String destinationCollection, String destinationURI) throws Exception{
		boolean status = false;
		HashMap<String, String> readOverrides = new HashMap<String, String>();
    	ReadConfig readConfig = null;
    	Map<String, String> writeOverrides = new HashMap<>();
    	double iTracker = 0.0;
    	try {
    		
    		MyLogger.log("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal : Start");
    		
            readOverrides.put(Constants.DATABASE, sourceDatabase);
            readOverrides.put(Constants.COLLECTION, sourceCollection);
            readOverrides.put(Constants.MONGO_HOST_URI, sourceURI);
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal : Load processing : " + readOverrides.toString() + " : Start");
            iTracker = 1.0;
            
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
           
            //
            iTracker = 2.0;
            
            Dataset<Row> dataSet = MongoSpark.load(jsc,readConfig).toDF();
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal : Load processing : " + readOverrides.toString() + " : End");
            
            iTracker = 3.0;
            
            String collectionYrMonthDay[] = sourceCollection.split("_");
            String yearMonthDay = "";
            String year="";
            String month="";
            int day=0;
            if(collectionYrMonthDay.length == 2){
            	yearMonthDay = collectionYrMonthDay[1];
	            year = yearMonthDay.substring(0, 4);
	            month = yearMonthDay.substring(4,6);
	            day = Integer.parseInt(yearMonthDay.substring(6));
            }
            
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal : dataSet.groupBy : Start");
            Dataset<Row> output = dataSet.groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID)
            		.count().withColumnRenamed("count", "d_"+day)
            		.withColumn(Constants.YEAR, functions.lit(year))
                    .withColumn(Constants.MONTH, functions.lit(month))
                    .withColumn(Constants.DAY, functions.lit(day))
                    .withColumn("_id", functions.lower(functions.concat(functions.col("institution_id"), functions.lit("~~"), 
                    		functions.col(Constants.JOURNAL_ID), functions.lit("~~"), 
                    		(functions.when(functions.col(Constants.ARTICLE_ID).contains("%").contains("{"), "invalidai").otherwise(functions.col(Constants.ARTICLE_ID))), functions.lit("~~"), 
                    		functions.col(Constants.PAGE_METRIC_ID),functions.lit("~~"),
                    		functions.col(Constants.ACCESS_METHOD),functions.lit("~~"),
                    		functions.lit(year),functions.lit("~~"),
                    		functions.lit(month))));
         
            //output.cache();
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal : dataSet.groupBy : End");
            
            iTracker = 5.0;
           
            String writeDatabase = destinationDatabase;
            String writeCollection = destinationCollection;
            writeOverrides.put(Constants.DATABASE, writeDatabase);
            writeOverrides.put(Constants.COLLECTION, writeCollection);
            writeOverrides.put("uri", destinationURI);
            writeOverrides.put("replaceDocument", "false");
            
            iTracker = 6.0;
            
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal : output.write() for : " + writeOverrides.toString() + " : Start");
            output.write().format("com.mongodb.spark.sql").mode("append").options(writeOverrides).save();
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal : output.write() for : " + writeOverrides.toString() + " : End");

            iTracker = 7.0;
            MyLogger.log("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal :  End");
            status = true;
            
            //******************************************************************************
		} catch (Exception e) {
			MyLogger.error("InstitutionStatsInsertion : insertInstStatsYearlyHorizontal : " + iTracker + " : " + e.toString());
		}
    	finally{    		
    			jsc.close();
    	}
    	return status;
	}
	
	
}
