package com.mps.c5.stats;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.mps.commons.AppConfig;
import com.mps.enums.MongoConfig;
import com.mps.mongo.MongoDBActions;
import com.mps.utils.Constants;
import com.mps.utils.Helper;
import com.mps.utils.MyLogger;

public class VsInstStats implements Serializable {

	private static final long serialVersionUID = 1L;

	private JavaSparkContext jsc;
	
	public VsInstStats(JavaSparkContext jsc) {
		this.jsc = jsc;
	}

	// Method for daily counter
	public boolean verticalStatsDailyToDaily() {
		Document currentProcess = AppConfig.getCurrentProcess();
		Document doc = InstitutionStats.getDocument();
		boolean status = true;

		try {
			
			String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			String statsDatabase = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String statsURI = AppConfig.get(MongoConfig.STATS_URI.getValue());

			String statsCollection = null;

			if (counterURI == null || counterDatabase == null || counterCollection == null) {
				MyLogger.error("HsInstStats: verticalStatsDailyToDaily(): skipping because :counterURI:" + counterURI
						+ ", counterDatabase:" + counterDatabase + ", counterCollection:" + counterCollection
						+ ", statsDatabase:" + statsDatabase + ", statsURI:" + statsURI);
				return false;
			}

			// If counter exists then proceed else return error
			if (!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
				MyLogger.error("VsInstStats: verticalStatsDailyToDaily: Collection does not exists, skipping further processing");
				return false;
			}

			String date = counterCollection.split("_")[1];
			// if date is available in collection name then insert daily stats
			// else skip
			if (date.length() == 8) {
				statsCollection = Constants.VERTICAL_STATS_DAILY_TO_DAILY + "_" + date;
				doc.replace("v_daily_to_daily_counter", statsCollection);
				doc.replace("v_daily_to_daily_start_time", Helper.getCurrentDateTime());
				MongoDBActions.updateDoc(statsURI, statsDatabase, AppConfig.get("statistics"), doc);

				InstitutionStatsInsertion dataInsertion = new InstitutionStatsInsertion(jsc);
				if (dataInsertion.verticalStatsDailyToDaily(counterDatabase, counterCollection, counterURI,	statsDatabase, statsCollection, statsURI)) {
					doc.replace("v_daily_to_daily_end_time", Helper.getCurrentDateTime());
					currentProcess.replace(Constants.VERTICAL_STATS_DAILY_TO_DAILY, 200);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
					MongoDBActions.updateDoc(statsURI, statsDatabase, AppConfig.get("statistics"), doc);
				} else {
					status = false;
					currentProcess.replace(Constants.VERTICAL_STATS_DAILY_TO_DAILY, -3);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
				}
			}

		} catch (Exception e) {
			MyLogger.error("vSInstStats: verticalStatsDailyToDaily(): EXCEPTION: " + e);
		}
		return status;
	}

	// Method for daily counter
	public boolean verticalStatsDailyToMonthly() {
		Document currentProcess = AppConfig.getCurrentProcess();
		Document doc = InstitutionStats.getDocument();
		boolean status = true;

		try {
			String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			String statsDatabase = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String statsURI = AppConfig.get(MongoConfig.STATS_URI.getValue());
			
			String statsCollection = null;

			if (counterURI == null || counterDatabase == null || counterCollection == null) {
				MyLogger.error("VsInstStats: verticalStatsDailyToMonthly(): skipping because :counterURI:" + counterURI
						+ ", counterDatabase:" + counterDatabase + ", counterCollection:" + counterCollection
						+ ", statsDatabase:" + statsDatabase + ", statsURI:" + statsURI);
				return false;
			}
			// If counter exists then proceed else return error
			if (!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
				MyLogger.error("VsInstStats: verticalStatsDailyToMonthly: Collection does not exists, skipping further processing");
				return false;
			}

			String date = counterCollection.split("_")[1];
			String year = date.substring(0, 4);
			String month = date.substring(4, 6);
			// if month available in collection name then insert daily
			// stats else skip this step
			if (month != null && month.length() == 2) {
				statsCollection = Constants.VERTICAL_STATS_DAILY_TO_MONTHLY + "_" + year + month;
				doc.replace("v_daily_to_monthly_counter", statsCollection);
				doc.replace("v_daily_to_monthly_start_time", Helper.getCurrentDateTime());
				MongoDBActions.updateDoc(statsURI, statsDatabase, AppConfig.get("statistics"), doc);

				InstitutionStatsInsertion dataInsertion = new InstitutionStatsInsertion(jsc);
				if (dataInsertion.verticalStatsDailyToMonthly(counterDatabase, counterCollection, counterURI, statsDatabase, statsCollection, statsURI)) {
					doc.replace("v_daily_to_monthly_end_time", Helper.getCurrentDateTime());
					currentProcess.replace(Constants.VERTICAL_STATS_DAILY_TO_MONTHLY, 200);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
					MongoDBActions.updateDoc(statsURI, statsDatabase, AppConfig.get("statistics"), doc);
				} else {
					status = false;
					currentProcess.replace(Constants.VERTICAL_STATS_DAILY_TO_MONTHLY, -3);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
				}
			}

		} catch (Exception e) {
			MyLogger.error("VsInstStats: verticalStatsDailyToMonthly(): EXCEPTION: " + e);
		}
		return status;
	}

	// Method for daily counter
	public boolean verticalStatsDailyToYearly() {
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

			Document instStatsDoc = new Document();

			if (counterURI == null || counterDatabase == null || counterCollection == null) {
				MyLogger.error("VsInstStats: verticalStatsDailyToYearly(): skipping because :counterURI:" + counterURI
						+ ", counterDatabase:" + counterDatabase + ", counterCollection:" + counterCollection
						+ ", statsDatabase:" + statsDatabase + ", statsURI:" + statsURI);
				return false;
			}
			// If counter exists then proceed else return error
			if (!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
				MyLogger.error("VsInstStats: verticalStatsDailyToYearly: Collection does not exists, skipping further processing");
				return false;
			}

			description = new StringBuilder();
			processingStartTime = new StringBuilder();
			processingEndTime = new StringBuilder();

			String date = counterCollection.split("_")[1];
			String year = date.substring(0, 4);
			// if year is available in collection name then insert daily
			// stats else skip this step
			if (year != null && year.length() == 4) {
				statsCollection = Constants.VERTICAL_STATS_DAILY_TO_YEARLY + "_" + year;
				processingStartTime.append(statsCollection).append("-").append("processingStartTime");
				instStatsDoc.append(processingStartTime.toString(), Helper.getCurrentDateTime());
				InstitutionStatsInsertion dataInsertion = new InstitutionStatsInsertion(jsc);
				if (dataInsertion.verticalStatsDailyToYearly(counterDatabase, counterCollection, counterURI, statsDatabase, statsCollection, statsURI)) {
					description.append(statsCollection).append("-").append("description");
					instStatsDoc.append(description.toString(), "WebLogCounter : insertInstStats() : SCCESS");
					currentProcess.replace(Constants.VERTICAL_STATS_DAILY_TO_YEARLY, 200);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
					// updateCounterStats();TODO
				} else {
					status = false;
					description.append(statsCollection).append("-").append("description");
					instStatsDoc.append(description.toString(), "WebLogCounter : insertInstStats() : ERROR");
					currentProcess.replace(Constants.VERTICAL_STATS_DAILY_TO_YEARLY, -3);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
					// updateCounterStats();TODO
				}
				processingEndTime.append(statsCollection).append("-").append("processingEndTime");
				instStatsDoc.append(processingEndTime.toString(), Helper.getCurrentDateTime());
				// updateCounterStats();
			}

		} catch (Exception e) {
			MyLogger.error("VsInstStats: verticalStatsDailyToYearly(): EXCEPTION: " + e);
		}

		return status;
	}

	// Method for monthly counter
	public boolean verticalStatsMonthlyToDaily() {
		Document currentProcess = AppConfig.getCurrentProcess();
		Document doc = InstitutionStats.getDocument();
		boolean status = true;

		try {
			String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			String statsDatabase = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String statsURI = AppConfig.get(MongoConfig.STATS_URI.getValue());
			
			if (counterURI == null || counterDatabase == null || counterCollection == null) {
				MyLogger.error("VsInstStats: verticalStatsDailyToYearly(): skipping because :counterURI:" + counterURI
						+ ", counterDatabase:" + counterDatabase + ", counterCollection:" + counterCollection
						+ ", statsDatabase:" + statsDatabase + ", statsURI:" + statsURI);
				return false;
			}

			// If counter exists then proceed else return error
			if (!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
				MyLogger.error("VsInstStats: verticalStatsDailyToYearly: Collection does not exists, skipping further processing");
				return false;
			}

			String date = counterCollection.split("_")[1];
			String year = date.substring(0, 4);
			String month = date.substring(4, 6);
			// if month available in collection name then insert daily
			// stats else skip this step
			if (month != null && month.length() == 2) {
				String statsCollection = Constants.VERTICAL_STATS_MONTHLY_TO_DAILY + "_" + year + month;
				doc.replace("v_monthly_to_daily_counter", statsCollection);
				doc.replace("v_monthly_to_daily_start_time", Helper.getCurrentDateTime());
				MongoDBActions.updateDoc(statsURI, statsDatabase, AppConfig.get("statistics"), doc);

				InstitutionStatsInsertion dataInsertion = new InstitutionStatsInsertion(jsc);
				if (dataInsertion.verticalStatsMonthlyToDaily(counterDatabase, counterCollection, counterURI, statsDatabase, statsCollection, statsURI)) {
					doc.replace("v_monthly_to_daily_end_time", Helper.getCurrentDateTime());
					currentProcess.replace(Constants.VERTICAL_STATS_MONTHLY_TO_DAILY, 200);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
					MongoDBActions.updateDoc(statsURI, statsDatabase, AppConfig.get("statistics"), doc);
				} else {
					status = false;
					currentProcess.replace(Constants.VERTICAL_STATS_MONTHLY_TO_DAILY, -3);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
				}
			}

		} catch (Exception e) {
			MyLogger.error("VsInstStats: verticalStatsDailyToYearly(): EXCEPTION: " + e);
		}
		return status;
	}

	// Method for monthly counter
	public boolean verticalStatsMonthlyToMonthly() {
		Document currentProcess = AppConfig.getCurrentProcess();
		Document doc = InstitutionStats.getDocument();
		boolean status = true;

		try {
			String counterURI = AppConfig.get(MongoConfig.COUNTER_URI.getValue());
			String counterDatabase = AppConfig.get(MongoConfig.COUNTER_DATABASE.getValue());
			String counterCollection = AppConfig.get(MongoConfig.COUNTER_COLLECTION.getValue());
			String statsDatabase = AppConfig.get(MongoConfig.STATS_DATABASE.getValue());
			String statsURI = AppConfig.get(MongoConfig.STATS_URI.getValue());
			
			if (counterURI == null || counterDatabase == null || counterCollection == null) {
				MyLogger.error("VsInstStats: verticalStatsDailyToYearly(): skipping because :counterURI:" + counterURI
						+ ", counterDatabase:" + counterDatabase + ", counterCollection:" + counterCollection
						+ ", statsDatabase:" + statsDatabase + ", statsURI:" + statsURI);
				return false;
			}

			// If counter exists then proceed else return error
			if (!MongoDBActions.isCollectionExists(counterURI, counterDatabase, counterCollection)) {
				MyLogger.error("VsInstStats: verticalStatsDailyToYearly: Collection does not exists, skipping further processing");
				return false;
			}

			String date = counterCollection.split("_")[1];
			String year = date.substring(0, 4);
			String month = date.substring(4, 6);
			// if month available in collection name then insert daily
			// stats else skip this step
			if (month != null && month.length() == 2) {
				String statsCollection = Constants.VERTICAL_STATS_MONTHLY_TO_MONTHLY + "_" + year + month;
				doc.replace("v_monthly_to_monthly_counter", statsCollection);
				doc.replace("v_monthly_to_monthly_start_time", Helper.getCurrentDateTime());
				MongoDBActions.updateDoc(statsURI, statsDatabase, AppConfig.get("statistics"), doc);
				InstitutionStatsInsertion dataInsertion = new InstitutionStatsInsertion(jsc);
				if (dataInsertion.verticalStatsMonthlyToMonthly(counterDatabase, counterCollection, counterURI,	statsDatabase, statsCollection, statsURI)) {
					doc.replace("v_monthly_to_monthly_end_time", Helper.getCurrentDateTime());
					currentProcess.replace(Constants.VERTICAL_STATS_MONTHLY_TO_MONTHLY, 200);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
					MongoDBActions.updateDoc(statsURI, statsDatabase, AppConfig.get("statistics"), doc);
				} else {
					status = false;
					currentProcess.replace(Constants.VERTICAL_STATS_MONTHLY_TO_MONTHLY, -3);
					MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
				}
			}

		} catch (Exception e) {
			MyLogger.error("VsInstStats: verticalStatsDailyToYearly(): EXCEPTION: " + e);
		}
		return status;
	}

}
