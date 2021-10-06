package com.mps.report.metric.stats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mps.c5.stats.MetricStats;
import com.mps.c5.stats.UserHit;
import com.mps.utils.Constants;
import com.mps.utils.MyLogger;

public class PRMetricStatsProcessor implements Serializable {

	private static final long serialVersionUID = 1L;
	
	static Document previousDoc;
	static String uniqueTitleInvestigation;
	static String uniqueTitleRequest;
	private static Document lastDocument;
	
	private static String uniqueId;
	private static long performanceCounter = 1000000;
	private static long recordProcessed;
	
	public static List<Document> getMetricStats(final Document currentDoc) {
		Double iTracker = 1.0;
		List<Document> docList = new ArrayList<>();
		try {
			
			if(uniqueId == null) {
				uniqueId = currentDoc.get(Constants.INSTITUTION_ID).toString().concat(currentDoc.get(Constants.JOURNAL_ID).toString());
			}
			iTracker = 1.1;
			if(previousDoc == null) {
				previousDoc = currentDoc;
			}
			
			if(uniqueId.equalsIgnoreCase(currentDoc.get(Constants.INSTITUTION_ID).toString().concat(currentDoc.get(Constants.JOURNAL_ID).toString()))) {
				MetricStats.setTotalItemInvestigation(1);
				iTracker = 1.2;
				if(previousDoc.get(Constants.SESSION_ID).toString().equalsIgnoreCase(currentDoc.get(Constants.SESSION_ID).toString())) {
					iTracker = 1.3;
					if(!previousDoc.get(Constants.ARTICLE_ID).toString().equalsIgnoreCase(currentDoc.get(Constants.ARTICLE_ID).toString())) {
						MetricStats.setUniqueItemInvestigations(1);
					}
				} else {
					MetricStats.setUniqueItemInvestigations(1);
				}
				iTracker = 1.4;
				if(UserHit.getRequestSet().contains(currentDoc.getInteger(Constants.PAGE_METRIC_ID))) {
					MetricStats.setTotalItemRequests(1);
					iTracker = 1.5;
					if(previousDoc.get(Constants.SESSION_ID).toString().equalsIgnoreCase(currentDoc.get(Constants.SESSION_ID).toString())) {
						iTracker = 1.6;
						if(!previousDoc.get(Constants.ARTICLE_ID).toString().equalsIgnoreCase(currentDoc.get(Constants.ARTICLE_ID).toString())) {
							iTracker = 1.7;
							MetricStats.setUniqueItemRequests(1);
						}
					} else {
						MetricStats.setUniqueItemRequests(1);
					}	
				}
				
				// unique title investigation/request
				iTracker = 1.8;
				if(!previousDoc.get(Constants.SESSION_ID).toString().equalsIgnoreCase(currentDoc.get(Constants.SESSION_ID).toString())) {
					iTracker = 1.9;
					MetricStats.setUniqueTitleInvestigation(1);
					
					if(UserHit.getRequestSet().contains(currentDoc.getInteger(Constants.PAGE_METRIC_ID))) {
						MetricStats.setUniqueTitleRequest(1);
					}
					
				} 
				iTracker = 2.0;
				uniqueId = currentDoc.get(Constants.INSTITUTION_ID).toString().concat(currentDoc.get(Constants.JOURNAL_ID).toString());
			}
			iTracker = 2.1;
			if(previousDoc.get(Constants.INSTITUTION_ID).toString().equalsIgnoreCase(currentDoc.get(Constants.INSTITUTION_ID).toString()) &&
					!previousDoc.get(Constants.JOURNAL_ID).toString().equalsIgnoreCase(currentDoc.get(Constants.JOURNAL_ID).toString())) {
				
				iTracker = 2.2;
				//Add previous stats to list
				
				MetricStats.setTotalItemInvestigation(1);
				MetricStats.setUniqueItemInvestigations(1);
				MetricStats.setUniqueTitleInvestigation(1);
				if(UserHit.getRequestSet().contains(currentDoc.getInteger(Constants.PAGE_METRIC_ID))) {
					MetricStats.setTotalItemRequests(1);
					MetricStats.setUniqueItemRequests(1);
					MetricStats.setUniqueTitleRequest(1);
				}
				iTracker = 2.3;
				uniqueId = currentDoc.get(Constants.INSTITUTION_ID).toString().concat(currentDoc.get(Constants.JOURNAL_ID).toString());
			}
			
			if(UserHit.getSearchesPlatform().contains(currentDoc.getInteger(Constants.PAGE_METRIC_ID))) {
				MetricStats.setSearchesPlatform(1);
			}
			
			if(!previousDoc.get(Constants.INSTITUTION_ID).toString().equalsIgnoreCase(currentDoc.get(Constants.INSTITUTION_ID).toString())) {
				docList.add(getPreviousDocWithStats());
				MetricStats.refresh();
				MetricStats.setTotalItemInvestigation(1);
				MetricStats.setUniqueItemInvestigations(1);
				MetricStats.setUniqueTitleInvestigation(1);
				if(UserHit.getRequestSet().contains(currentDoc.getInteger(Constants.PAGE_METRIC_ID))) {
					MetricStats.setTotalItemRequests(1);
					MetricStats.setUniqueItemRequests(1);
					MetricStats.setUniqueTitleRequest(1);
				}
				if(UserHit.getSearchesPlatform().contains(currentDoc.getInteger(Constants.PAGE_METRIC_ID))) {
					MetricStats.setSearchesPlatform(1);
				}
				uniqueId = currentDoc.get(Constants.INSTITUTION_ID).toString().concat(currentDoc.get(Constants.JOURNAL_ID).toString());
			}
			previousDoc = currentDoc;
		
		} catch (Exception e) {
			MyLogger.error("MetricStatsProcessor: getMetricStats: iTracker: " + iTracker + " EXCEPTION: " + e);
		}
		lastDocument = currentDoc;
		recordProcessed++;
		if(recordProcessed % performanceCounter == 0) {
			MyLogger.log("MetricStatsProcessor: getMetricStats: Total records processed till yet: " + recordProcessed);
		}
		return docList;
	}
	
	private static Document getPreviousDocWithStats() {
		Document document = new Document();
		try {
			document.
			append(Constants.INSTITUTION_ID, previousDoc.get(Constants.INSTITUTION_ID).toString()).
			append(Constants.YEAR, previousDoc.get(Constants.YEAR).toString()).
			append(Constants.MONTH, previousDoc.get(Constants.MONTH).toString()).
			append("total_item_investigation", MetricStats.getTotalItemInvestigation()).
			append("unique_item_investigation", MetricStats.getUniqueItemInvestigations()).
			append("unique_title_investigation", MetricStats.getUniqueTitleInvestigation()).
			append("total_item_request", MetricStats.getTotalItemRequests()).
			append("unique_item_request", MetricStats.getUniqueItemRequests()).
			append("unique_title_request", MetricStats.getUniqueTitleRequest()).
			append("searches_platform", MetricStats.getSearchesPlatform());
		} catch(Exception e) {
			MyLogger.error("MetricStatsProcessor: getPreviousDocWithStats: EXCEPTION: " + e);
		}
		return document;
	}
	public static Document getLastDocument() {
		return lastDocument;
	}

	public static void setLastDocument(Document lastDocument) {
		PRMetricStatsProcessor.lastDocument = lastDocument;
	}
	
}
