package com.mps.c5.stats;

import java.io.Serializable;

public class MetricStats implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private static int totalItemInvestigation;
	private static int uniqueItemInvestigations;
	private static int uniqueTitleInvestigation;
	
	private static int totalItemRequests;
	private static int uniqueItemRequests;
	private static int uniqueTitleRequest;
	
	private static int searchesPlatform;
	
	public static void refresh() {
		totalItemInvestigation = 0;
		uniqueItemInvestigations = 0;
		uniqueTitleInvestigation = 0;
		
		totalItemRequests = 0;
		uniqueItemRequests = 0;
		uniqueTitleRequest = 0;
		
		searchesPlatform = 0;
	}
	public static int getTotalItemInvestigation() {
		return totalItemInvestigation;
	}
	public static void setTotalItemInvestigation(int totalItemInvestigation) {
		MetricStats.totalItemInvestigation = MetricStats.totalItemInvestigation + totalItemInvestigation;
	}
	public static int getUniqueItemInvestigations() {
		return uniqueItemInvestigations;
	}
	public static void setUniqueItemInvestigations(int uniqueItemInvestigations) {
		MetricStats.uniqueItemInvestigations = MetricStats.uniqueItemInvestigations + uniqueItemInvestigations;
	}
	public static int getUniqueTitleInvestigation() {
		return uniqueTitleInvestigation;
	}
	public static void setUniqueTitleInvestigation(int uniqueTitleInvestigation) {
		MetricStats.uniqueTitleInvestigation = MetricStats.uniqueTitleInvestigation + uniqueTitleInvestigation;
	}
	public static int getTotalItemRequests() {
		return totalItemRequests;
	}
	public static void setTotalItemRequests(int totalItemRequests) {
		MetricStats.totalItemRequests = MetricStats.totalItemRequests + totalItemRequests;
	}
	public static int getUniqueItemRequests() {
		return uniqueItemRequests;
	}
	public static void setUniqueItemRequests(int uniqueItemRequests) {
		MetricStats.uniqueItemRequests = MetricStats.uniqueItemRequests + uniqueItemRequests;
	}
	public static int getUniqueTitleRequest() {
		return uniqueTitleRequest;
	}
	public static void setUniqueTitleRequest(int uniqueTitleRequest) {
		MetricStats.uniqueTitleRequest = MetricStats.uniqueTitleRequest + uniqueTitleRequest;
	}
	public static int getSearchesPlatform() {
		return searchesPlatform;
	}
	public static void setSearchesPlatform(int searchesPlatform) {
		MetricStats.searchesPlatform = MetricStats.searchesPlatform + searchesPlatform;
	}
	
	
}
