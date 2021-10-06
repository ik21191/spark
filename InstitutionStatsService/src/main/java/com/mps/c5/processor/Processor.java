package com.mps.c5.processor;

import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.mps.c5.stats.HsInstIpStats;
import com.mps.c5.stats.HsInstStats;
import com.mps.c5.stats.InstitutionStats;
import com.mps.c5.stats.ReportStats;
import com.mps.c5.stats.VsInstStats;
import com.mps.commons.AppConfig;
import com.mps.enums.MongoConfig;
import com.mps.mongo.MongoDBActions;
import com.mps.spark.MySpark;
import com.mps.utils.MyLogger;

public class Processor {
	
	private JavaSparkContext jsc;
	
	public Processor() throws Exception {
		jsc = new MySpark().getJavaSparkContext();
	}
	
	public void run() throws Exception {
		Document currentProcess = AppConfig.getCurrentProcess();
		boolean status = true;
		try {
			InstitutionStats.setInitial();
			MongoDBActions.insertDoc(AppConfig.get(MongoConfig.STATS_URI.getValue()), AppConfig.get(MongoConfig.STATS_DATABASE.getValue()), AppConfig.get(MongoConfig.STATISTCS.getValue()), InstitutionStats.getDocument());
			
			// create vs daily institution stats if required
	    	MyLogger.log("Processor: run(): STATUS VERTICAL DAILY TO DAILY : " + Integer.parseInt(AppConfig.get("vs_stats_daily_to_daily")));
	    	if(Integer.parseInt(AppConfig.get("vs_stats_daily_to_daily")) == 1) {
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL DAILY TO DAILY : Start");
	    		VsInstStats vsInstStats = new VsInstStats(jsc);
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL DAILY TO DAILY : End with status : " + vsInstStats.verticalStatsDailyToDaily());
	    	}
	    	
			// create vs monthly institution stats if required
	    	MyLogger.log("Processor: run(): STATUS VERTICAL DAILY TO MONTHLY  : " + Integer.parseInt(AppConfig.get(MongoConfig.VS_STATS_DAILY_TO_MONTHLY.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.VS_STATS_DAILY_TO_MONTHLY.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL DAILY TO MONTHLY : Start");
	    		VsInstStats vsInstStats = new VsInstStats(jsc);
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL DAILY TO MONTHLY : End with status : " + vsInstStats.verticalStatsDailyToMonthly());
	    	}
	    	
			// create vs yearly institution stats if required
	    	MyLogger.log("Processor: run(): STATUS VERTICAL DAILY TO YEARLY : " + Integer.parseInt(AppConfig.get(MongoConfig.VS_STATS_DAILY_TO_YEARLY.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.VS_STATS_DAILY_TO_YEARLY.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL DAILY TO YEARLY : Start");
	    		VsInstStats vsInstStats = new VsInstStats(jsc);
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL DAILY TO YEARLY : End with status : " + vsInstStats.verticalStatsDailyToYearly());
	    	}
	    	
	    	// create vs yearly institution stats if required
	    	MyLogger.log("Processor: run(): STATUS VERTICAL MONTHLY TO DAILY : " + Integer.parseInt(AppConfig.get(MongoConfig.VS_STATS_MONTHLY_TO_DAILY.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.VS_STATS_MONTHLY_TO_DAILY.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL MONTHLY TO DAILY : Start");
	    		VsInstStats vsInstStats = new VsInstStats(jsc);
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL MONTHLY TO DAILY : End with status : " + vsInstStats.verticalStatsMonthlyToDaily());
	    	}
	    	
	    	// create vs yearly institution stats if required
	    	MyLogger.log("Processor: run(): STATUS VERTICAL MONTHLY TO MONTHLY : " + Integer.parseInt(AppConfig.get(MongoConfig.VS_STATS_MONTHLY_TO_MONTHLY.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.VS_STATS_MONTHLY_TO_MONTHLY.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL MONTHLY TO MONTHLY : Start");
	    		VsInstStats vsInstStats = new VsInstStats(jsc);
	    		MyLogger.log("Processor: run(): Creating Institution Stats VERTICAL MONTHLY TO MONTHLY : End with status : " + vsInstStats.verticalStatsMonthlyToMonthly());
	    	}
	    	
	    	// create hs monthly institution stats if required
	    	MyLogger.log("Processor: run(): HORIZONTAL MONTHLY : " + Integer.parseInt(AppConfig.get(MongoConfig.HS_STATS_MONTHLY.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.HS_STATS_MONTHLY.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): Creating Institution Stats Horizontal Monthly : Start");
	    		HsInstStats hsInstStats = new HsInstStats(jsc);
	    		MyLogger.log("Processor: run(): Creating Institution Stats Horizontal Monthly : End with status : " + hsInstStats.insertInstStatsMonthlyHorizontal());
	    	}
	    	
	    	// create hs yearly institution stats if required
	    	MyLogger.log("Processor: run(): Status code Horizontal Stats Yearly : " + Integer.parseInt(AppConfig.get(MongoConfig.HS_STATS_YEARLY.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.HS_STATS_YEARLY.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): Creating Institution Stats Horizontal Yearly : Start");
	    		HsInstStats hsInstStats = new HsInstStats(jsc);
	    		MyLogger.log("Processor: run(): Creating Institution Stats Horizontal Yearly : End with status : " + hsInstStats.insertInstStatsYearlyHorizontal());
	    	}
	    	
	    	//Insert institution IP stats
	    	MyLogger.log("Processor: run(): Status code Horizontal IP Stats Monthly : " + Integer.parseInt(AppConfig.get(MongoConfig.HS_IP_STATS_MONTHLY.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.HS_IP_STATS_MONTHLY.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): Creating Institution IP Stats Horizontal Monthly : Start");
	    		HsInstIpStats hsInstIpStats = new HsInstIpStats(jsc);
	    		MyLogger.log("Processor: run(): Creating Institution IP Stats Horizontal Monthly : End with status : " + hsInstIpStats.dataInsertMonthlyHorizontal());
	    	}
	    	
	    	int irStatsGroupby = Integer.parseInt(AppConfig.get(MongoConfig.IR_REPORT_STATS_GROUPBY.getValue()));
	    	int trStats = Integer.parseInt(AppConfig.get(MongoConfig.TR_REPORT_STATS.getValue()));
	    	int irStats = Integer.parseInt(AppConfig.get(MongoConfig.IR_REPORT_STATS.getValue()));
	    	int drStats = 0;
	    	int prStats = Integer.parseInt(AppConfig.get(MongoConfig.PR_REPORT_STATS.getValue()));
	    	
	    	if(trStats != 0 || irStats != 0 || drStats != 0 || prStats != 0 || irStatsGroupby != 0) {
	    		ReportStats reportStats = new ReportStats(jsc);
	    		status = reportStats.createReportStats();
	    	}
	    	
	    	// Save TR Report stats to MySql
	    	MyLogger.log("Processor: run(): Status code TR stats to MySql : " + Integer.parseInt(AppConfig.get(MongoConfig.TR_TO_MYSQL.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.TR_TO_MYSQL.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): inserting stats to MySql: Start");
	    		ReportStats reportStats = new ReportStats(jsc);
	    		reportStats.initialize();
	    		MyLogger.log("Processor: run(): inserting stats to MySql: Start " + reportStats.updateTRMaster());
			}
	    	
	    	// Save IR Report stats to MySql
	    	MyLogger.log("Processor: run(): Status code IR stats to MySql : " + Integer.parseInt(AppConfig.get(MongoConfig.IR_TO_MYSQL.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.IR_TO_MYSQL.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): inserting stats to MySql: Start");
	    		ReportStats reportStats = new ReportStats(jsc);
	    		reportStats.initialize();
	    		reportStats.initializeFeedArticle();
	    		MyLogger.log("Processor: run(): inserting stats to MySql: Start " + reportStats.updateIRMaster());
			}
	    	
	    	// Save PR Report stats to MySql
	    	MyLogger.log("Processor: run(): Status code PR stats to MySql : " + Integer.parseInt(AppConfig.get(MongoConfig.PR_TO_MYSQL.getValue())));
	    	if(Integer.parseInt(AppConfig.get(MongoConfig.PR_TO_MYSQL.getValue())) == 1) {
	    		MyLogger.log("Processor: run(): inserting stats to MySql: Start");
	    		ReportStats reportStats = new ReportStats(jsc);
	    		reportStats.initialize();
	    		MyLogger.log("Processor: run(): inserting stats to MySql: Start " + reportStats.updatePRMaster());
			}
	    	
	    } catch(Exception e) {
	    	status = false;
	    	currentProcess.replace("status", -3);
			MyLogger.error("Processor: run(): EXCEPTION: " + e);
		} finally {
			if(status) {
				currentProcess.replace("status", 200);
	    	} else {
	    		currentProcess.replace("status", 400);
	    	}
			MongoDBActions.updateDoc(AppConfig.get(MongoConfig.MONGO_HOST.getValue()), AppConfig.get(MongoConfig.MONGO_DATABASE.getValue()), AppConfig.get(MongoConfig.MONGO_COLLECTION.getValue()), currentProcess);
		}
		if(jsc != null) {
			jsc.stop();
		}
			
	}
}
