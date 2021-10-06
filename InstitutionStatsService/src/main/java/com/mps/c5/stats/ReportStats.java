package com.mps.c5.stats;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mps.commons.AppConfig;
import com.mps.enums.MongoConfig;
import com.mps.feeder.RDDGeneric;
import com.mps.mongo.MongoDBActions;
import com.mps.report.metric.stats.IRMetricStatsProcessor;
import com.mps.report.metric.stats.PRMetricStatsProcessor;
import com.mps.report.metric.stats.TRMetricStatsProcessor;
import com.mps.stats.beans.IrMaster;
import com.mps.stats.beans.PrMaster;
import com.mps.stats.beans.TrMaster;
import com.mps.stats.comparator.IRMasterSortDocument;
import com.mps.stats.comparator.TRMasterSortDocument;
import com.mps.utils.Constants;
import com.mps.utils.MyException;
import com.mps.utils.MyLogger;
import com.mysql.data.report.SaveReportStats;

import scala.Option;
import scala.Tuple2;

public class ReportStats implements Serializable {

	private static final long serialVersionUID = 1L;
	private JavaSparkContext jsc;
	
	private static Set<String> journalIdSet;
	private static Map<String, Document> journalMap = new HashMap<>();
	private static Map<String, Document> institutionMap = new HashMap<>();
	private static Set<TrMaster> trMasterSet = new HashSet<>();
	private static Set<IrMaster> irMasterSet = new HashSet<>();
	private static Set<PrMaster> prMasterSet = new HashSet<>();
	private static long performanceCounter = 30000;
	private static long totalAccountProcessed;
	
	static scala.collection.immutable.HashMap feedArticlesMap = new scala.collection.immutable.HashMap<>();
	
	public ReportStats(JavaSparkContext jsc) throws Exception{
		Double iTracker = 1.0;		
		try {
			this.jsc = jsc;
			iTracker = 1.1;
			if(jsc == null){
				throw new MyException("ReportStats: ReportStats: Invalid Java Spark Context : " + jsc);
			}
		} catch (Exception e) {
			MyLogger.exception("ReportStats: ReportStats: iTracker: " + iTracker + " : " + e.toString());
		}
	}
	/**We are not using this feature as of now*/
	public boolean createTRMaster() {
		boolean status = true;
		Document currentProcess = AppConfig.getCurrentProcess();
		MyLogger.log("ReportStats: createTRMaster(): Start");
		Double iTracker = 0.0;
		try {
			
			HashMap<String, String> readOverrides = new HashMap<>();
	    	ReadConfig readConfig = null;
			
	    	String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			
	    	readOverrides.put(Constants.MONGO_HOST_URI, counterURI);
	    	readOverrides.put(Constants.DATABASE, counterDatabase);
            readOverrides.put(Constants.COLLECTION, counterCollection);
            iTracker = 1.0;
            if(!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
            	MyLogger.error("ReportStats: createTRMaster() : COLLECTION: " + counterCollection + " doesn't exists or contains zero records.");
            	return false;
            }
            
            MyLogger.log("ReportStats: createTRMaster() : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.1;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
        
            iTracker = 1.2;
            Dataset<Row> counterDataSet = MongoSpark.load(jsc, readConfig).toDF().
            		select(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.YEAR, Constants.MONTH);
            MyLogger.log("ReportStats : createTRMaster(): Load Processing : " + readOverrides + " : End");
            iTracker = 1.3;
            
            Dataset<Row> filteredDataSet = counterDataSet.filter(row->journalIdSet.contains(row.getAs(Constants.JOURNAL_ID).toString()));
            
            iTracker = 1.4;
            
            Dataset<Row> dataSetGroupBy = filteredDataSet.
            		groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.YEAR, Constants.MONTH).count();
            
			iTracker = 1.5;
     	    
			String uri = AppConfig.get("stats_uri");
			String db = AppConfig.get("stats_database");
			String collection = AppConfig.get("tr_report_stats_collection");
			
			Map<String, String> writeOverrides = new HashMap<>();
			writeOverrides.put(Constants.MONGO_HOST_URI, uri);
	        writeOverrides.put(Constants.DATABASE, db);
	        writeOverrides.put(Constants.COLLECTION, collection);
	        iTracker = 1.6;
	        
	        MyLogger.log("ReportStats: createTRMaster(): SAVING: " + writeOverrides + " :Start");
	        dataSetGroupBy.write().format("com.mongodb.spark.sql").mode("append").options(writeOverrides).save();
	        iTracker = 1.7;
	        currentProcess.replace(Constants.STATUS_TR_REPORT_STATS, 200);
	        MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			iTracker = 1.8;
	        MyLogger.log("ReportStats: createTRMaster(): SAVING: " + writeOverrides + " :End");
	        MyLogger.log("ReportStats: createTRMaster(): End");
	   } catch (Exception e) {
		   	status = false;
			MyLogger.exception("ReportStats: createTRMaster(): iTracker: " + iTracker + " EXCEPTION: " + e);
			currentProcess.replace(Constants.STATUS_TR_REPORT_STATS, -3);
			try {
				MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			} catch (Exception e1) {
				MyLogger.error("ReportStats: createTRMaster: EXCEPTION: " + e1);
			}
		} finally {
			jsc.close();
			jsc.stop();
		}
		return status;
	}
	
	public boolean createIRMaster() {
		Document currentProcess = AppConfig.getCurrentProcess();
		boolean status = true;
		MyLogger.log("ReportStats: createIRMaster(): Start");
		Double iTracker = 0.0;
		try {
			
			HashMap<String, String> readOverrides = new HashMap<>();
	    	ReadConfig readConfig = null;
	    	
	    	String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			
	    	readOverrides.put(Constants.MONGO_HOST_URI, counterURI);
	    	readOverrides.put(Constants.DATABASE, counterDatabase);
            readOverrides.put(Constants.COLLECTION, counterCollection);
            MyLogger.log("ReportStats: createIRMaster() : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            
            iTracker = 1.1;
            if(!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
            	MyLogger.error("ReportStats: createIRMaster() : COLLECTION: " + counterCollection + " doesn't exists or contains zero records.");
            	return false;
            }
            
            iTracker = 1.2;
            Dataset<Row> counterDataSet = MongoSpark.load(jsc, readConfig).toDF().
            		select(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.SESSION_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.YEAR, Constants.MONTH);
            MyLogger.log("ReportStats : createIRMaster(): Load Processing : " + readOverrides + " : End");
            iTracker = 1.3;
            
            Dataset<Row> dataSetGroupBy = counterDataSet.
            		groupBy(Constants.INSTITUTION_ID, Constants.JOURNAL_ID, Constants.SESSION_ID, Constants.ARTICLE_ID, Constants.PAGE_METRIC_ID, Constants.ACCESS_METHOD, Constants.YEAR, Constants.MONTH).count();
            
			iTracker = 1.4;
     	    
			String uri = AppConfig.get(MongoConfig.STATS_URI.getValue());
			String db = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String collection = AppConfig.get(MongoConfig.IR_REPORT_STATS_COLLECTION_GROUPBY.getValue());
			
			Map<String, String> writeOverrides = new HashMap<>();
			writeOverrides.put(Constants.MONGO_HOST_URI, uri);
	        writeOverrides.put(Constants.DATABASE, db);
	        writeOverrides.put(Constants.COLLECTION, collection);
	        iTracker = 1.5;
	        
	        MyLogger.log("ReportStats: createIRMaster(): SAVING: " + writeOverrides + " :Start");
	        dataSetGroupBy.write().format("com.mongodb.spark.sql").mode("append").options(writeOverrides).save();
	        iTracker = 1.6;
	        currentProcess.replace(Constants.STATUS_IR_REPORT_STATS_GROUPBY, 200);
	        MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			iTracker = 1.7;
	        MyLogger.log("ReportStats: createIRMaster(): SAVING: " + writeOverrides + " :End");
	        MyLogger.log("ReportStats: createIRMaster(): End");
	   } catch (Exception e) {
		   	status = false;
			MyLogger.exception("ReportStats: createIRMaster(): iTracker: " + iTracker + " EXCEPTION: " + e);
			currentProcess.replace(Constants.STATUS_IR_REPORT_STATS_GROUPBY, -3);
			try {
				MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			} catch (Exception e1) {
				MyLogger.error("ReportStats: createIRMaster: EXCEPTION: " + e1);
			}
		} finally {
			jsc.close();
			jsc.stop();
		}
		return status;
	}
	
	private JavaRDD<Document> getSortedCounterForTR() {
		Double iTracker = 0.0;
		JavaRDD<Document> sortedCounterRDD = null;
		try {
			HashMap<String, String> readOverrides = new HashMap<>();
	    	ReadConfig readConfig = null;
			
	    	String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			
	    	readOverrides.put(Constants.MONGO_HOST_URI, counterURI);
	    	readOverrides.put(Constants.DATABASE, counterDatabase);
            readOverrides.put(Constants.COLLECTION, counterCollection);
            MyLogger.log("ReportStats: getSortedCounterForTR() : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            
            iTracker = 1.1;
            if(!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
            	MyLogger.error("ReportStats: getSortedCounterForTR() : COLLECTION: " + counterCollection + " doesn't exists or contains zero records.");
            	return null;
            }
            
            iTracker = 1.2;
            JavaRDD<Document> counterRDD = MongoSpark.load(jsc, readConfig);
            iTracker = 1.3;
            MyLogger.log("ReportStats: getSortedCounterForTR(): Sorting javaRDD : Start");
            
            JavaPairRDD<Document, Integer> javaPairRdd = counterRDD.mapToPair(document->new Tuple2<>(document, 1));
            iTracker = 1.4;
            Comparator<Document> comparator = new TRMasterSortDocument();
            JavaPairRDD<Document, Integer> javaPairRddSorted = javaPairRdd.sortByKey(comparator);
            iTracker = 1.5;
            
            sortedCounterRDD = javaPairRddSorted.map(tuple->tuple._1);
            MyLogger.log("ReportStats: getSortedCounterForTR(): Sorting javaRDD : End");
		} catch(Exception e) {
			MyLogger.error("ReportStats: getSortedCounterForTR(): iTracker: " + iTracker + " EXCEPTION: " + e);
		}
		return sortedCounterRDD;
	}
	
	private JavaRDD<Document> getSortedCounterForIR() {
		Double iTracker = 0.0;
		JavaRDD<Document> sortedCounterRDD = null;
		try {
			HashMap<String, String> readOverrides = new HashMap<>();
	    	ReadConfig readConfig = null;
			
	    	String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			
	    	readOverrides.put(Constants.MONGO_HOST_URI, counterURI);
	    	readOverrides.put(Constants.DATABASE, counterDatabase);
            readOverrides.put(Constants.COLLECTION, counterCollection);
            MyLogger.log("ReportStats: getSortedCounterForIR() : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            
            iTracker = 1.1;
            if(!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
            	MyLogger.error("ReportStats: getSortedCounterForIR() : COLLECTION: " + counterCollection + " doesn't exists or contains zero records.");
            	return null;
            }
            
            iTracker = 1.2;
            JavaRDD<Document> counterRDD = MongoSpark.load(jsc, readConfig);
            iTracker = 1.3;
            MyLogger.log("ReportStats: getSortedCounterForIR(): Sorting javaRDD : Start");
            
            JavaPairRDD<Document, Integer> javaPairRdd = counterRDD.mapToPair(document->new Tuple2<>(document, 1));
            iTracker = 1.4;
            Comparator<Document> comparator = new IRMasterSortDocument();
            JavaPairRDD<Document, Integer> javaPairRddSorted = javaPairRdd.sortByKey(comparator);
            iTracker = 1.5;
            
            sortedCounterRDD = javaPairRddSorted.map(tuple->tuple._1);
            MyLogger.log("ReportStats: getSortedCounterForIR(): Sorting javaRDD : End");
		} catch(Exception e) {
			MyLogger.error("ReportStats: getSortedCounterForIR(): iTracker: " + iTracker + " EXCEPTION: " + e);
		}
		return sortedCounterRDD;
	}
	
	public boolean createReportStats() {
		boolean status = false;
		try {
			int irStatsGroupby = Integer.parseInt(AppConfig.get(MongoConfig.IR_REPORT_STATS_GROUPBY.getValue()));
			int trStats = Integer.parseInt(AppConfig.get(MongoConfig.TR_REPORT_STATS.getValue()));
	    	int irStats = Integer.parseInt(AppConfig.get(MongoConfig.IR_REPORT_STATS.getValue()));
	    	int drStats = 0;
	    	int prStats = Integer.parseInt(AppConfig.get(MongoConfig.PR_REPORT_STATS.getValue()));;
	    	
	    	if(trStats == 1 || prStats == 1) {
	    		JavaRDD<Document> sortedCounterRDD = getSortedCounterForTR();
	    		
	    		if(trStats == 1) {
	    			if(sortedCounterRDD == null) {
		    			return false;
		    		}
		    		MyLogger.log("ReportStats: createReportStats: Creating TR Report stats Start");
		    		status = createTotalUniqueStatsForTRReport(sortedCounterRDD);
		    		MyLogger.log("ReportStats: createReportStats: Creating TR Report stats End with status: " + status);
	    		}
	    		
	    		if(prStats == 1) {
	    			if(sortedCounterRDD == null) {
		    			return false;
		    		}
		    		MyLogger.log("ReportStats: createReportStats: Creating PR Report stats Start");
		    		status = createTotalUniqueStatsForPRReport(sortedCounterRDD);
		    		MyLogger.log("ReportStats: createReportStats: Creating PR Report stats End with status: " + status);
	    		}
	    		jsc.close();
				jsc.stop();
	    	}
	    	
	    	if(irStats == 1) {
	    		JavaRDD<Document> sortedCounterRDDForIR = getSortedCounterForIR();
	    		if(sortedCounterRDDForIR == null) {
	    			return false;
	    		}
	    		MyLogger.log("ReportStats: createReportStats: Creating IR Report stats Start");
	    		status = createTotalUniqueStatsForIRReport(sortedCounterRDDForIR);
	    		MyLogger.log("ReportStats: createReportStats: Creating IR Report stats End with status: " + status);
	    	}
	    	
	    	//Creating IR report stats group by for Kapil report
	    	if(irStatsGroupby == 1) {
	    		MyLogger.log("ReportStats: createReportStats: Creating IR Report stats groupby : Start");
	    		status = createIRMaster();
	    		MyLogger.log("ReportStats: createReportStats: Creating IR Report stats End with status : " + status);
	    	}
	    	
	    	status = true;
		} catch(Exception e) {
			MyLogger.error("ReportStats: createReportStats: EXCEPTION: " + e);
		}
		return status;
	}
	
	private boolean createTotalUniqueStatsForTRReport(JavaRDD<Document> sortedCounterRDD) {
		boolean status = true;
		Document currentProcess = AppConfig.getCurrentProcess();
		MyLogger.log("ReportStats: createTotalUniqueStatsForTRReport(): Start");
		Double iTracker = 0.0;
		try {
			iTracker = 1.1;
			JavaRDD<Document> finalRDD = sortedCounterRDD.flatMap((final Document doc) -> TRMetricStatsProcessor.getMetricStats(doc).iterator());
			iTracker = 1.2;
            
            String uri = AppConfig.get(MongoConfig.STATS_URI.getValue());
			String db = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String collection = AppConfig.get(MongoConfig.TR_REPORT_STATS_COLLECTION.getValue());
			
			Map<String, String> writeOverrides = new HashMap<>();
			writeOverrides.put(Constants.MONGO_HOST_URI, uri);
	        writeOverrides.put(Constants.DATABASE, db);
	        writeOverrides.put(Constants.COLLECTION, collection);
	        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
	        
	        iTracker = 1.3;
	        if(MongoDBActions.isCollectionExists(uri, db, collection)) {
	        	MyLogger.log("ReportStats: createTotalUniqueStatsForTRReport(): collection: " + collection + "exists, renaming it");
	        	MongoDBActions.renameCollection(uri, db, collection, collection + "_" + new SimpleDateFormat("yyyyMMddhhmmss").format(new Date()));
	        }
	        iTracker = 1.4;
	        
	        MyLogger.log("ReportStats: createTotalUniqueStatsForTRReport(): SAVING: " + writeOverrides + " :Start");
	        MongoSpark.save(finalRDD, writeConfig);
	        iTracker = 1.5;
	        currentProcess.replace(Constants.STATUS_TR_REPORT_STATS, 200);
	        MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
	        iTracker = 1.6;
			MyLogger.log("ReportStats: createTotalUniqueStatsForTRReport(): SAVING: " + writeOverrides + " :End");
	        MyLogger.log("ReportStats: createTotalUniqueStatsForTRReport(): End");
	   } catch (Exception e) {
		   	status = false;
			MyLogger.exception("ReportStats: createTotalUniqueStatsForTRReport(): iTracker: " + iTracker + " EXCEPTION: " + e);
			currentProcess.replace(Constants.STATUS_TR_REPORT_STATS, -3);
			try {
				MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			} catch (Exception e1) {
				MyLogger.error("ReportStats: createTotalUniqueStatsForTRReport: EXCEPTION: " + e1);
			}
		} 
		return status;
	}
	
	private boolean createTotalUniqueStatsForIRReport(JavaRDD<Document> sortedCounterRDD) {
		boolean status = true;
		Document currentProcess = AppConfig.getCurrentProcess();
		MyLogger.log("ReportStats: createTotalUniqueStatsForIRReport(): Start");
		Double iTracker = 0.0;
		try {
			iTracker = 1.1;
			JavaRDD<Document> finalRDD = sortedCounterRDD.flatMap((final Document doc) -> IRMetricStatsProcessor.getMetricStats(doc).iterator());
			iTracker = 1.2;
            
			String uri = AppConfig.get(MongoConfig.STATS_URI.getValue());
			String db = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String collection = AppConfig.get(MongoConfig.IR_REPORT_STATS_COLLECTION.getValue());
			
			Map<String, String> writeOverrides = new HashMap<>();
			writeOverrides.put(Constants.MONGO_HOST_URI, uri);
	        writeOverrides.put(Constants.DATABASE, db);
	        writeOverrides.put(Constants.COLLECTION, collection);
	        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
	        iTracker = 1.3;
	        if(MongoDBActions.isCollectionExists(uri, db, collection)) {
	        	MyLogger.log("ReportStats: createTotalUniqueStatsForIRReport(): collection: " + collection + "exists, renaming it");
	        	MongoDBActions.renameCollection(uri, db, collection, collection + "_" + new SimpleDateFormat("yyyyMMddhhmmss").format(new Date()));
	        }
	        iTracker = 1.4;
	        
	        MyLogger.log("ReportStats: createTotalUniqueStatsForIRReport(): SAVING: " + writeOverrides + " :Start");
	        MongoSpark.save(finalRDD, writeConfig);
	        iTracker = 1.5;
	        currentProcess.replace(Constants.STATUS_IR_REPORT_STATS, 200);
	        MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
	        iTracker = 1.6;
			MyLogger.log("ReportStats: createTotalUniqueStatsForIRReport(): SAVING: " + writeOverrides + " :End");
	        MyLogger.log("ReportStats: createTotalUniqueStatsForIRReport(): End");
	   } catch (Exception e) {
		   	status = false;
			MyLogger.exception("ReportStats: createTotalUniqueStatsForIRReport(): iTracker: " + iTracker + " EXCEPTION: " + e);
			currentProcess.replace(Constants.STATUS_IR_REPORT_STATS, -3);
			try {
				MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			} catch (Exception e1) {
				MyLogger.error("ReportStats: createTotalUniqueStatsForIRReport: EXCEPTION: " + e1);
			}
		} finally {
			jsc.close();
			jsc.stop();
		}
		return status;
	}
	
	private boolean createTotalUniqueStatsForPRReport(JavaRDD<Document> sortedCounterRDD) {
		boolean status = true;
		Document currentProcess = AppConfig.getCurrentProcess();
		MyLogger.log("ReportStats: createTotalUniqueStatsForPRReport(): Start");
		Double iTracker = 0.0;
		try {
			iTracker = 1.1;
			JavaRDD<Document> finalRDD = sortedCounterRDD.flatMap((final Document doc) -> PRMetricStatsProcessor.getMetricStats(doc).iterator());
			iTracker = 1.2;
            
			String uri = AppConfig.get(MongoConfig.STATS_URI.getValue());
			String db = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String collection = AppConfig.get(MongoConfig.PR_REPORT_STATS_COLLECTION.getValue());
			
			Map<String, String> writeOverrides = new HashMap<>();
			writeOverrides.put(Constants.MONGO_HOST_URI, uri);
	        writeOverrides.put(Constants.DATABASE, db);
	        writeOverrides.put(Constants.COLLECTION, collection);
	        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
	        iTracker = 1.3;
	        if(MongoDBActions.isCollectionExists(uri, db, collection)) {
	        	MyLogger.log("ReportStats: createTotalUniqueStatsForPRReport(): collection: " + collection + "exists, renaming it");
	        	MongoDBActions.renameCollection(uri, db, collection, collection + "_" + new SimpleDateFormat("yyyyMMddhhmmss").format(new Date()));
	        }
	        iTracker = 1.4;
	        
	        MyLogger.log("ReportStats: createTotalUniqueStatsForPRReport(): SAVING: " + writeOverrides + " :Start");
	        MongoSpark.save(finalRDD, writeConfig);
	        iTracker = 1.5;
	        currentProcess.replace(Constants.STATUS_PR_REPORT_STATS, 200);
	        MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
	        iTracker = 1.6;
			MyLogger.log("ReportStats: createTotalUniqueStatsForPRReport(): SAVING: " + writeOverrides + " :End");
	        MyLogger.log("ReportStats: createTotalUniqueStatsForPRReport(): End");
	   } catch (Exception e) {
		   	status = false;
			MyLogger.exception("ReportStats: createTotalUniqueStatsForPRReport(): iTracker: " + iTracker + " EXCEPTION: " + e);
			currentProcess.replace(Constants.STATUS_PR_REPORT_STATS, -3);
			try {
				MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			} catch (Exception e1) {
				MyLogger.error("ReportStats: createTotalUniqueStatsForPRReport: EXCEPTION: " + e1);
			}
		}
		return status;
	}
	
	
	public void initialize() throws MyException{
		Double iTracker=0.0;
		HashMap<String, String> filterMap = new HashMap<>();
		try {
			iTracker = 1.0;
			filterMap = new HashMap<>();
			filterMap.put("generate_reports", "1");
			String feederURI = AppConfig.get(MongoConfig.FEEDER_URI.getValue());
			String feederDB = AppConfig.get(MongoConfig.FEEDER_DATABASE.getValue());
			
			filterMap = new HashMap<>();
			filterMap.put("collection_type", "Any");
			String feedJournalCollection = AppConfig.get(MongoConfig.DATE.getValue()) + "_" + AppConfig.get(MongoConfig.FEED_JOURNALS.getValue());
			iTracker = 1.1;
			MyLogger.log("ReportStats: Initilization : URI : "+ feederURI +" : database : " + feederDB + " : collection : " + feedJournalCollection + " : " + filterMap.toString());
			RDDGeneric.initialize(jsc, feederURI, feederDB, feedJournalCollection);
			iTracker = 1.2;
			
			journalMap = RDDGeneric.getFilteredPairedRDDMap(filterMap, "journal_id", RDDGeneric.KEY_CASE.LOWER); //in lower case, false for Case sensitive
			if (journalMap == null || journalMap.size() <= 0) {
				throw new Exception("Feed Journals : NULL/ZERO journalMap Records");
			}
			MyLogger.log("ReportStats: Initilization: Total journals fetched: " + journalMap.size());
			iTracker = 1.3;
		
			initializeJournalList(journalMap);
			iTracker = 1.4;

			// setting account map
			String feederAccount = AppConfig.get(MongoConfig.DATE.getValue()) + "_" + AppConfig.get(MongoConfig.ACCOUNTS.getValue());
			RDDGeneric.initialize(jsc, feederURI, feederDB, feederAccount);
			MyLogger.log("ReportStats: Initilization : URI : "+ feederURI +" : database : " + feederDB + " : collection : " + feederAccount);
			institutionMap = RDDGeneric.getPairedRDDMap("code", RDDGeneric.KEY_CASE.LOWER);
			MyLogger.log("ReportStats: Initilization: Total account fetched: " + institutionMap.size());
		
			RDDGeneric.destroy();
			
		} catch (Exception e) {
			throw new MyException("ReportStats: initialize() : iTracker : " + iTracker + " : " + e.toString());
		}
	}
	
	public void initializeFeedArticle() throws MyException{
		Double iTracker=0.0;
		try {
			
			String feederURI = AppConfig.get(MongoConfig.FEEDER_URI.getValue());
			String feederDB = AppConfig.get(MongoConfig.FEEDER_DATABASE.getValue());
			String feedArticle = AppConfig.get(MongoConfig.DATE.getValue()) + "_" + AppConfig.get(MongoConfig.ARTICLE.getValue());
			MyLogger.log("ReportStats: initializeFeedArticle : URI : "+ feederURI +" : database : " + feederDB + " : collection : " + feedArticle);
			Map<String, String> readOverrides = new HashMap<>();
			readOverrides.put(Constants.MONGO_HOST_URI, feederURI);
			readOverrides.put(Constants.DATABASE, feederDB);
			readOverrides.put(Constants.COLLECTION, feedArticle);
			
			iTracker=0.1;
			JavaRDD<Document> javaRDDFeedAticles = MongoSpark.load(jsc, ReadConfig.create(jsc).withOptions(readOverrides));
			iTracker=0.2;
			MyLogger.log("ReportStats: initializeFeedArticle: Adding key value in feedArticlesMap : Start");
			javaRDDFeedAticles.foreach(doc-> {
				try {
					Tuple2<String, Document> keyValue = new Tuple2<>(doc.get(Constants.ARTICLE_ID).toString().trim().toLowerCase(), doc);
					feedArticlesMap = feedArticlesMap.$plus(keyValue);
				}catch(Exception e) {
					MyLogger.error("ReportStats: RDD foreach: EXCEPTION: " + e);
				}
			});
			iTracker=0.3;
			MyLogger.log("ReportStats: initializeFeedArticle: Adding key value in feedArticlesMap : End. Total count in feedArticleMap is : " + feedArticlesMap.size());
		} catch (Exception e) {
			throw new MyException("ReportStats: initializeFeedArticle() : iTracker : " + iTracker + " : " + e.toString());
		}
	}
	
	public void initializeJournalList(Map<String, Document> journalMap) {
		Set<String> set = new HashSet<>();
		
		for(String journalId : journalMap.keySet()) {
			set.add(journalId);
		}
		
		MyLogger.log("ReportStats: initializeJournalList: Total JournalId count: " + set.size());
		ReportStats.journalIdSet = set;
	}
	
	
	
	public boolean updateTRMaster() {
		boolean status = false;
		Document currentProcess = AppConfig.getCurrentProcess();
		double iTracker = 0.0;
		try {
			HashMap<String, String> readOverrides = new HashMap<>();
	    	ReadConfig readConfig = null;
	    	
	    	String statsURI = AppConfig.get(MongoConfig.STATS_URI.getValue());
			String statsDatabase = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String trStatsCollection = AppConfig.get(MongoConfig.TR_REPORT_STATS_COLLECTION.getValue());
			
			readOverrides.put(Constants.MONGO_HOST_URI, statsURI);
	    	readOverrides.put(Constants.DATABASE, statsDatabase);
            readOverrides.put(Constants.COLLECTION, trStatsCollection);
            MyLogger.log("ReportStats: updateTRMaster() : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            
            iTracker = 1.1;
            if(!MongoDBActions.isCollectionExists(statsURI, statsDatabase, trStatsCollection)) {
            	MyLogger.error("ReportStats: updateTRMaster() : COLLECTION: " + trStatsCollection + " doesn't exists or contains zero records.");
            	return false;
            }
            
            iTracker = 1.2;
            JavaRDD<Document> trStatsRDD = MongoSpark.load(jsc, readConfig);
            
            trStatsRDD.foreach(doc-> {
            	totalAccountProcessed++;
            	updateJournalDetails(doc);
            	if(totalAccountProcessed % performanceCounter == 0) {
            		SaveReportStats.setDynamicColumn("M_" + AppConfig.get("date"));
            		SaveReportStats.saveTRMaster(trMasterSet);
            		MyLogger.log("ReportStats: updateTRMaster(): FOREACH: Total records processed till yet: " + totalAccountProcessed + ", trMasterSet.size : " + trMasterSet.size());
            		trMasterSet.clear();
            	}
            });
            
            if(!trMasterSet.isEmpty()) {
            	MyLogger.log("ReportStats: updateTRMaster(): Processing PENDING records out of total records: " + totalAccountProcessed + ", trMasterSet.size : " + trMasterSet.size());
            	SaveReportStats.setDynamicColumn("M_" + AppConfig.get(MongoConfig.DATE.getValue()));
            	SaveReportStats.saveTRMaster(trMasterSet);
            }
            status = true;
            currentProcess.replace(Constants.TR_TO_MYSQL, 200);    
		}catch(Exception e) {
			status = false;
			MyLogger.exception("ReportStats: updateTRMaster(): iTracker: " + iTracker + " EXCEPTION: " + e.getStackTrace());
			currentProcess.replace(Constants.TR_TO_MYSQL, -3);
			try {
				MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			} catch (Exception e1) {
				MyLogger.error("ReportStats: updateTRMaster: EXCEPTION: " + e1);
			}
			MyLogger.error("ReportStats: updateTRMaster: iTracker: " + iTracker + " EXCEPTION: " + e);
		} finally {
			jsc.close();
			jsc.stop();
		}
		return status;
	}
	
	public boolean updateIRMaster() {
		totalAccountProcessed = 0;
		boolean status = false;
		Document currentProcess = AppConfig.getCurrentProcess();
		double iTracker = 0.0;
		try {
			HashMap<String, String> readOverrides = new HashMap<>();
	    	ReadConfig readConfig = null;
	    	
	    	String statsURI = AppConfig.get(MongoConfig.STATS_URI.getValue());
			String statsDatabase = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String irStatsCollection = AppConfig.get(MongoConfig.IR_REPORT_STATS_COLLECTION.getValue());
			
			readOverrides.put(Constants.MONGO_HOST_URI, statsURI);
	    	readOverrides.put(Constants.DATABASE, statsDatabase);
            readOverrides.put(Constants.COLLECTION, irStatsCollection);
            MyLogger.log("ReportStats: updateIRMaster() : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            
            iTracker = 1.1;
            if(!MongoDBActions.isCollectionExists(statsURI, statsDatabase, irStatsCollection)) {
            	MyLogger.error("ReportStats: updateIRMaster() : COLLECTION: " + irStatsCollection + " doesn't exists or contains zero records.");
            	return false;
            }
            
            iTracker = 1.2;
            JavaRDD<Document> irStatsRDD = MongoSpark.load(jsc, readConfig);
            
            irStatsRDD.foreach(doc-> {
            	totalAccountProcessed++;
            	updateArticleDetails(doc);
            	if(totalAccountProcessed % performanceCounter == 0) {
            		SaveReportStats.setDynamicColumn("M_" + AppConfig.get(MongoConfig.DATE.getValue()));
            		SaveReportStats.saveIRMaster(irMasterSet);
            		MyLogger.log("ReportStats: updateIRMaster(): FOREACH: Total records processed till yet: " + totalAccountProcessed + ", irMasterSet.size : " + irMasterSet.size());
            		irMasterSet.clear();
            	}
            });
            
            if(!irMasterSet.isEmpty()) {
            	MyLogger.log("ReportStats: updateIRMaster(): Processing PENDING records out of total records: " + totalAccountProcessed + ", irMasterSet.size : " + irMasterSet.size());
            	SaveReportStats.setDynamicColumn("M_" + AppConfig.get(MongoConfig.DATE.getValue()));
            	SaveReportStats.saveIRMaster(irMasterSet);
            }
            status = true;
            currentProcess.replace(Constants.IR_TO_MYSQL, 200);    
		}catch(Exception e) {
			status = false;
			MyLogger.exception("ReportStats: updateIRMaster(): iTracker: " + iTracker + " EXCEPTION: " + e.getStackTrace());
			currentProcess.replace(Constants.IR_TO_MYSQL, -3);
			try {
				MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			} catch (Exception e1) {
				MyLogger.error("ReportStats: updateIRMaster: EXCEPTION: " + e1);
			}
			MyLogger.error("ReportStats: updateIRMaster: iTracker: " + iTracker + " EXCEPTION: " + e);
		} finally {
			jsc.close();
			jsc.stop();
		}
		return status;
	}
	
	public boolean updatePRMaster() {
		totalAccountProcessed = 0;
		boolean status = false;
		Document currentProcess = AppConfig.getCurrentProcess();
		double iTracker = 0.0;
		try {
			HashMap<String, String> readOverrides = new HashMap<>();
	    	ReadConfig readConfig = null;
	    	
	    	String statsURI = AppConfig.get(MongoConfig.STATS_URI.getValue());
			String statsDatabase = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String prStatsCollection = AppConfig.get(MongoConfig.PR_REPORT_STATS_COLLECTION.getValue());
			
			readOverrides.put(Constants.MONGO_HOST_URI, statsURI);
	    	readOverrides.put(Constants.DATABASE, statsDatabase);
            readOverrides.put(Constants.COLLECTION, prStatsCollection);
            MyLogger.log("ReportStats: updatePRMaster() : Load Processing : " + readOverrides + " : Start");
            iTracker = 1.0;
            readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
            
            iTracker = 1.1;
            if(!MongoDBActions.isCollectionExists(statsURI, statsDatabase, prStatsCollection)) {
            	MyLogger.error("ReportStats: updatePRMaster() : COLLECTION: " + prStatsCollection + " doesn't exists or contains zero records.");
            	return false;
            }
            
            iTracker = 1.2;
            JavaRDD<Document> prStatsRDD = MongoSpark.load(jsc, readConfig);
            
            prStatsRDD.foreach(doc-> {
            	totalAccountProcessed++;
            	updatePrMasterDetails(doc);
            	if(totalAccountProcessed % performanceCounter == 0) {
            		SaveReportStats.setDynamicColumn("M_" + AppConfig.get(MongoConfig.DATE.getValue()));
            		SaveReportStats.savePRMaster(prMasterSet);
            		MyLogger.log("ReportStats: updatePRMaster(): FOREACH: Total records processed till yet: " + totalAccountProcessed + ", prMasterSet.size : " + prMasterSet.size());
            		prMasterSet.clear();
            	}
            });
            
            if(!prMasterSet.isEmpty()) {
            	MyLogger.log("ReportStats: updatePRMaster(): Processing PENDING records out of total records: " + totalAccountProcessed + ", prMasterSet.size : " + prMasterSet.size());
            	SaveReportStats.setDynamicColumn("M_" + AppConfig.get(MongoConfig.DATE.getValue()));
            	SaveReportStats.savePRMaster(prMasterSet);
            }
            status = true;
            currentProcess.replace(Constants.PR_TO_MYSQL, 200);    
		}catch(Exception e) {
			status = false;
			MyLogger.exception("ReportStats: updatePRMaster(): iTracker: " + iTracker + " EXCEPTION: " + e.getStackTrace());
			currentProcess.replace(Constants.PR_TO_MYSQL, -3);
			try {
				MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
			} catch (Exception e1) {
				MyLogger.error("ReportStats: updatePRMaster: EXCEPTION: " + e1);
			}
			MyLogger.error("ReportStats: updatePRMaster: iTracker: " + iTracker + " EXCEPTION: " + e);
		} finally {
			jsc.close();
			jsc.stop();
		}
		return status;
	}
	
	private static TrMaster updateJournalDetails(Document doc) throws Exception {
		TrMaster trMaster = new TrMaster();
		try {
			String journalId = doc.get(Constants.JOURNAL_ID, "-");
			
			if(!journalId.equals("-")) {
				trMaster.setJournalID(journalId);
				Document journalDoc = journalMap.get(journalId.trim().toLowerCase());
				if(journalDoc != null) {
					trMaster.setTitle(journalDoc.get(Constants.JOURNAL_TITLE, "-"));
					trMaster.setPublisher(journalDoc.get(Constants.PUBLISHER, "-"));
					trMaster.setPlatform(journalDoc.get(Constants.PLATFORM, "-"));
					trMaster.setDoi(journalDoc.get(Constants.JOURNAL_DOI, "-"));
					trMaster.setPrintISSN(journalDoc.get(Constants.PRINT_ISSN, "-"));
					trMaster.setOnlineISSN(journalDoc.get(Constants.ONLINE_ISSN, "-"));
				}
			}
			
			String institutionId = doc.get(Constants.INSTITUTION_ID, "-");
			if(!institutionId.equals("-")) {
				trMaster.setInstitutionID(institutionId);
				Document institutionDoc = institutionMap.get(institutionId.trim().toLowerCase());
				if(institutionDoc != null) {
					trMaster.setInstitutionName(institutionDoc.get(Constants.INSTITUTION_NAME, "-"));
				}
			}
			validateTRMaster(trMaster);
			//update all metric types count in trMaster
			
			//total item investigation
			trMaster.setMetricType("Total_Item_Investigations");
			trMaster.setMetricCount(doc.getInteger(Constants.TOTAL_ITEM_INVESTIGATION).intValue());
			if(trMaster.getMetricCount() != 0) {
				trMasterSet.add((TrMaster)trMaster.clone());
			}
			//unique item investigation
			trMaster.setMetricType("Unique_Item_Investigations");
			trMaster.setMetricCount(doc.getInteger(Constants.UNIQUE_ITEM_INVESTIGATION).intValue());
			if(trMaster.getMetricCount() != 0) {
				trMasterSet.add((TrMaster)trMaster.clone());
			}
			//unique title investigation
			trMaster.setMetricType("Unique_Title_Investigations");
			trMaster.setMetricCount(doc.getInteger(Constants.UNIQUE_TITLE_INVESTIGATION).intValue());
			if(trMaster.getMetricCount() != 0) {
				trMasterSet.add((TrMaster)trMaster.clone());
			}
			//total item request
			trMaster.setMetricType("Total_Item_Requests");
			trMaster.setMetricCount(doc.getInteger(Constants.TOTAL_ITEM_REQUEST).intValue());
			if(trMaster.getMetricCount() != 0) {
				trMasterSet.add((TrMaster)trMaster.clone());
			}
			//unique item request
			trMaster.setMetricType("Unique_Item_Requests");
			trMaster.setMetricCount(doc.getInteger(Constants.UNIQUE_ITEM_REQUEST).intValue());
			if(trMaster.getMetricCount() != 0) {
				trMasterSet.add((TrMaster)trMaster.clone());
			}
			//unique title request
			trMaster.setMetricType("Unique_Title_Requests");
			trMaster.setMetricCount(doc.getInteger(Constants.UNIQUE_TITLE_REQUEST).intValue());
			if(trMaster.getMetricCount() != 0) {
				trMasterSet.add((TrMaster)trMaster.clone());
			}
		}catch(Exception e) {
			MyLogger.error("ReportStats: updateJournalDetails: EXCEPTION: " + e);
			throw e;
		}
		return trMaster;
	}
	
	
	private static IrMaster updateArticleDetails(Document currentDoc) throws Exception {
		double iTracker = 0.0;
		IrMaster irMaster = new IrMaster();
		try {
			
			String articleId = currentDoc.get(Constants.ARTICLE_ID, "-");
			iTracker = 1.0;
			if(!articleId.equals("-")) {
				irMaster.setArticleID(articleId);
				iTracker = 1.0;
				Option<Document> articleOptionDoc =  feedArticlesMap.get(articleId.trim().toLowerCase());
				iTracker = 1.1;
				if(!articleOptionDoc.isEmpty()) {
					iTracker = 1.2;
	    	    	Document articleDoc = articleOptionDoc.get();
	    	    	iTracker = 1.3;
	    	    	updateArticleDetails(irMaster, articleDoc);
	    	    	iTracker = 1.4;
	    	    	updateParentDetails(irMaster, articleDoc.get(Constants.JOURNAL_ID).toString());
	    	    	iTracker = 1.5;
	    	    	updateInstitutionDetails(irMaster, currentDoc.get(Constants.INSTITUTION_ID).toString());
	    	    	iTracker = 1.6;
	    	    } 
				
			} else {
				irMaster.setArticleID(articleId);
			}
			
			iTracker = 1.7;
			validateIRMaster(irMaster);
			iTracker = 1.8;
			//update all metric types count in trMaster
			
			//total item investigation
			irMaster.setMetricType("Total_Item_Investigations");
			irMaster.setMetricCount(currentDoc.getInteger(Constants.TOTAL_ITEM_INVESTIGATION).intValue());
			if(irMaster.getMetricCount() != 0) {
				irMasterSet.add((IrMaster)irMaster.clone());
			}
			//unique item investigation
			irMaster.setMetricType("Unique_Item_Investigations");
			irMaster.setMetricCount(currentDoc.getInteger(Constants.UNIQUE_ITEM_INVESTIGATION).intValue());
			if(irMaster.getMetricCount() != 0) {
				irMasterSet.add((IrMaster)irMaster.clone());
			}
			
			//total item request
			irMaster.setMetricType("Total_Item_Requests");
			irMaster.setMetricCount(currentDoc.getInteger(Constants.TOTAL_ITEM_REQUEST).intValue());
			if(irMaster.getMetricCount() != 0) {
				irMasterSet.add((IrMaster)irMaster.clone());
			}
			//unique item request
			irMaster.setMetricType("Unique_Item_Requests");
			irMaster.setMetricCount(currentDoc.getInteger(Constants.UNIQUE_ITEM_REQUEST).intValue());
			if(irMaster.getMetricCount() != 0) {
				irMasterSet.add((IrMaster)irMaster.clone());
			}
			
		}catch(Exception e) {
			MyLogger.error("ReportStats: updateJournalDetails: iTracker: " + iTracker + " EXCEPTION: " + e);
			throw e;
		}
		return irMaster;
	}
	
	private static void updateInstitutionDetails(IrMaster irMaster, String institutionId) {
		try {
			if(!institutionId.equals("-")) {
				irMaster.setInstitutionID(institutionId);
				Document institutionDoc = institutionMap.get(institutionId.trim().toLowerCase());
				if(institutionDoc != null) {
					irMaster.setInstitutionName(institutionDoc.get(Constants.INSTITUTION_NAME, "-"));
				}
			}
		}catch(Exception e) {
			MyLogger.error("ReportStats: updateInstitutionDetails: EXCEPTION: " + e);
		}
		
	}
	
	private static PrMaster updatePrMasterDetails(Document currentDoc) throws Exception {
		double iTracker = 0.0;
		PrMaster prMaster = new PrMaster();
		try {
			String institutionId = currentDoc.get(Constants.INSTITUTION_ID, "-");
			if(!institutionId.equals("-")) {
				prMaster.setInstitutionID(institutionId);
				Document institutionDoc = institutionMap.get(institutionId.trim().toLowerCase());
				if(institutionDoc != null) {
					prMaster.setInstitutionName(institutionDoc.get(Constants.INSTITUTION_NAME, "-"));
				}
			} else {
				prMaster.setInstitutionID(institutionId);
			}
			
			iTracker = 1.7;
			validatePRMaster(prMaster);
			iTracker = 1.8;
			//update all metric types count in trMaster
			
			//total item investigation
			prMaster.setMetricType("Total_Item_Investigations");
			prMaster.setMetricCount(currentDoc.getInteger(Constants.TOTAL_ITEM_INVESTIGATION).intValue());
			if(prMaster.getMetricCount() != 0) {
				prMasterSet.add((PrMaster)prMaster.clone());
			}
			//unique item investigation
			prMaster.setMetricType("Unique_Item_Investigations");
			prMaster.setMetricCount(currentDoc.getInteger(Constants.UNIQUE_ITEM_INVESTIGATION).intValue());
			if(prMaster.getMetricCount() != 0) {
				prMasterSet.add((PrMaster)prMaster.clone());
			}
			//unique title investigation
			prMaster.setMetricType("Unique_Title_Investigations");
			prMaster.setMetricCount(currentDoc.getInteger(Constants.UNIQUE_TITLE_INVESTIGATION).intValue());
			if(prMaster.getMetricCount() != 0) {
				prMasterSet.add((PrMaster)prMaster.clone());
			}
			//total item request
			prMaster.setMetricType("Total_Item_Requests");
			prMaster.setMetricCount(currentDoc.getInteger(Constants.TOTAL_ITEM_REQUEST).intValue());
			if(prMaster.getMetricCount() != 0) {
				prMasterSet.add((PrMaster)prMaster.clone());
			}
			//unique item request
			prMaster.setMetricType("Unique_Item_Requests");
			prMaster.setMetricCount(currentDoc.getInteger(Constants.UNIQUE_ITEM_REQUEST).intValue());
			if(prMaster.getMetricCount() != 0) {
				prMasterSet.add((PrMaster)prMaster.clone());
			}
			//unique title request
			prMaster.setMetricType("Unique_Title_Requests");
			prMaster.setMetricCount(currentDoc.getInteger(Constants.UNIQUE_TITLE_REQUEST).intValue());
			if(prMaster.getMetricCount() != 0) {
				prMasterSet.add((PrMaster)prMaster.clone());
			}
			
			//searches platform
			prMaster.setMetricType("Search_Platform");
			prMaster.setMetricCount(currentDoc.getInteger(Constants.SEARCHES_PLATFORM).intValue());
			if(prMaster.getMetricCount() != 0) {
				prMasterSet.add((PrMaster)prMaster.clone());
			}
			
		}catch(Exception e) {
			MyLogger.error("ReportStats: updatePrMasterDetails: iTracker: " + iTracker + " EXCEPTION: " + e);
			throw e;
		}
		return prMaster;
	}
	
	private static void updateParentDetails(IrMaster irMaster, String journalId) {
		try {
			if(!journalId.equals("-")) {
				irMaster.setJournalID(journalId);
				Document journalDoc = journalMap.get(journalId.trim().toLowerCase());
				if(journalDoc != null) {
					irMaster.setParentTitle(journalDoc.get(Constants.JOURNAL_TITLE, "-"));
					irMaster.setParentDOI(journalDoc.get(Constants.JOURNAL_DOI, "-"));
					irMaster.setParentPrintISSN(journalDoc.get(Constants.PRINT_ISSN, "-"));
					irMaster.setParentOnlineISSN(journalDoc.get(Constants.ONLINE_ISSN, "-"));
				}
			}
		}catch(Exception e) {
			MyLogger.error("ReportStats: updateInstitutionDetails: EXCEPTION: " + e);
		}
		
	}
	
	public static void updateArticleDetails(IrMaster irMaster, Document articleDoc) {
		try {
			irMaster.setItem(articleDoc.get("title", "-"));	
			irMaster.setAuthors(articleDoc.get("author", "-"));
			irMaster.setDoi(articleDoc.get("doi", "-"));
		} catch (Exception e) {
			MyLogger.error("ReportStats: updateInstitutionDetails: EXCEPTION: " + e);
		}

	}
	
	private static void validateIRMaster(IrMaster irMaster) {
		if(irMaster.getInstitutionID().length() > 255) {
			irMaster.setInstitutionID(irMaster.getInstitutionID().substring(0, 254));
		}
		if(irMaster.getInstitutionName().length() > 512) {
			irMaster.setInstitutionName(irMaster.getInstitutionName().substring(0, 511));
		}
		if(irMaster.getJournalID().length() > 255) {
			irMaster.setJournalID(irMaster.getJournalID().substring(0, 254));
		}
		if(irMaster.getItem().length() > 512) {
			irMaster.setItem(irMaster.getItem().substring(0, 511));
		}
	}
	
	private static void validatePRMaster(PrMaster prMaster) {
		if(prMaster.getInstitutionID().length() > 255) {
			prMaster.setInstitutionID(prMaster.getInstitutionID().substring(0, 254));
		}
		if(prMaster.getInstitutionName().length() > 512) {
			prMaster.setInstitutionName(prMaster.getInstitutionName().substring(0, 511));
		}
	}
	
	private static void validateTRMaster(TrMaster trMaster) {
		if(trMaster.getInstitutionID().length() > 255) {
			trMaster.setInstitutionID(trMaster.getInstitutionID().substring(0, 254));
		}
		if(trMaster.getInstitutionName().length() > 512) {
			trMaster.setInstitutionName(trMaster.getInstitutionName().substring(0, 511));
		}
		if(trMaster.getJournalID().length() > 255) {
			trMaster.setJournalID(trMaster.getJournalID().substring(0, 254));
		}
		if(trMaster.getTitle().length() > 512) {
			trMaster.setTitle(trMaster.getTitle().substring(0, 511));
		}
	}
}