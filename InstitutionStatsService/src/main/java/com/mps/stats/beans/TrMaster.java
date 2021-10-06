package com.mps.stats.beans;

import java.io.Serializable;

public class TrMaster implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;
	
	private String institutionID = "-";
	private String institutionName = "-";
	private String journalID = "-";
	private String title = "-";
	private String publisher = "-";
	private String publisherID = "-";
	private String platform = "-";
	private String doi = "-";
	private String proprietaryID = "-";
	private String isbn = "-";
	private String printISSN = "-";
	private String onlineISSN = "-";
	private String uri = "-";
	private String dataType = "-";
	private String sectionType = "-";
	private String accessType = "Controlled";
	private String accessMethod = "Regular";
	private String metricType = "Search_Automated";
	private int reportingPeriodTotal;
	private String timeStamp = "2018-02-19";
	private String yop = "-";
	private int metricCount;
	
	public String getInstitutionID() {
		return institutionID;
	}
	public void setInstitutionID(String institutionID) {
		this.institutionID = institutionID;
	}
	public String getInstitutionName() {
		return institutionName;
	}
	public void setInstitutionName(String institutionName) {
		this.institutionName = institutionName;
	}
	public String getJournalID() {
		return journalID;
	}
	public void setJournalID(String journalID) {
		this.journalID = journalID;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getPublisher() {
		return publisher;
	}
	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}
	public String getPublisherID() {
		return publisherID;
	}
	public void setPublisherID(String publisherID) {
		this.publisherID = publisherID;
	}
	
	public String getDoi() {
		return doi;
	}
	public void setDoi(String doi) {
		this.doi = doi;
	}
	public String getProprietaryID() {
		return proprietaryID;
	}
	public void setProprietaryID(String proprietaryID) {
		this.proprietaryID = proprietaryID;
	}
	
	public String getPrintISSN() {
		return printISSN;
	}
	public void setPrintISSN(String printISSN) {
		this.printISSN = printISSN;
	}
	public String getOnlineISSN() {
		return onlineISSN;
	}
	public void setOnlineISSN(String onlineISSN) {
		this.onlineISSN = onlineISSN;
	}
	
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getSectionType() {
		return sectionType;
	}
	public void setSectionType(String sectionType) {
		this.sectionType = sectionType;
	}
	public String getAccessMethod() {
		return accessMethod;
	}
	public void setAccessMethod(String accessMethod) {
		this.accessMethod = accessMethod;
	}
	public String getMetricType() {
		return metricType;
	}
	public void setMetricType(String metricType) {
		this.metricType = metricType;
	}
	
	public String getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getIsbn() {
		return isbn;
	}
	public void setIsbn(String isbn) {
		this.isbn = isbn;
	}
	public String getYop() {
		return yop;
	}
	public void setYop(String yop) {
		this.yop = yop;
	}
	public String getAccessType() {
		return accessType;
	}
	public void setAccessType(String accessType) {
		this.accessType = accessType;
	}
	public int getMetricCount() {
		return metricCount;
	}
	public void setMetricCount(int metricCount) {
		this.metricCount = metricCount;
	}
	
	public Object clone() throws CloneNotSupportedException{
		return super.clone();
	}
	public int getReportingPeriodTotal() {
		return reportingPeriodTotal;
	}
	public void setReportingPeriodTotal(int reportingPeriodTotal) {
		this.reportingPeriodTotal = reportingPeriodTotal;
	}
	
}
