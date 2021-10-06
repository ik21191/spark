package com.mps.c5.stats;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.mps.commons.AppConfig;
import com.mps.enums.MongoConfig;
import com.mps.mongo.MongoDBActions;
import com.mps.utils.Helper;
import com.mps.utils.MyLogger;

public class HsInstIpStats implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private JavaSparkContext jsc;
	private Document taskDoc;
	
	public HsInstIpStats(JavaSparkContext jsc) {
		this.jsc = jsc;
	}
	
	public boolean dataInsertMonthlyHorizontal() {
		Document currentProcess = AppConfig.getCurrentProcess();
		boolean status = true;
		
		
		try {
			String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			String statsDatabase = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String statsURI = AppConfig.get(MongoConfig.STATS_URI.getValue());
			
			String statsCollection = null;
			StringBuilder processingStartTime = new StringBuilder();
			StringBuilder description = new StringBuilder();
			StringBuilder processingEndTime = new StringBuilder();
			
			Document instIPStatsDoc = new Document();
			
			if(counterURI == null || counterDatabase == null || counterCollection == null) {
				MyLogger.error("HsInstStats: insertInstStatsMonthlyVertical(): skipping because :counterURI:" + counterURI + ", counterDatabase:" + counterDatabase + ", counterCollection:" + counterCollection +
						", statsDatabase:" + statsDatabase + ", statsURI:" + statsURI);
				return false;
			}

			String date = counterCollection.split("_")[1];
			String year = date.substring(0, 4);
			String month = date.substring(4, 6);
			// day should be available in collection name to perform
			// this task, as we insert daily wise data in column
			if (date.length() == 8) {
				statsCollection = "hs_ip_stats_monthly" + "_" + year + month;
				processingStartTime.append(statsCollection).append("-").append("processingStartTime");
				instIPStatsDoc.append(processingStartTime.toString(), Helper.getCurrentDateTime());
				InstitutionIPStats institutionIPStats = new InstitutionIPStats(jsc);
				if (institutionIPStats.dataInsertMonthlyHorizontal(counterDatabase, counterCollection, counterURI, statsDatabase, statsCollection, statsURI)) {
					description.append(statsCollection).append("-").append("description");
					instIPStatsDoc.append(description.toString(), "WebLogCounter : insertInstIPStats() : SCCESS");
					taskDoc.replace("hs_ip_stats_monthly", 200);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
					//updateCounterStats();TODO
				} else {
					status = false;
					description.append(statsCollection).append("-").append("description");
					instIPStatsDoc.append(description.toString(), "WebLogCounter : insertInstIPStats() : ERROR");
					taskDoc.replace("hs_ip_stats_monthly", -3);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
					//updateCounterStats();TODO
				}
				processingEndTime.append(statsCollection).append("-").append("processingEndTime");
				instIPStatsDoc.append(processingEndTime.toString(), Helper.getCurrentDateTime());
				//updateCounterStats();TODO
			}
		
		}catch(Exception e) {
			MyLogger.error("HsInstIpStats: dataInsertMonthlyHorizontal(): EXCEPTION: " + e);
		}
		return status;
	}

}
