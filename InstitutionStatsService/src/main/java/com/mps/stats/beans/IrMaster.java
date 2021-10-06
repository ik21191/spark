package com.mps.stats.beans;

import java.io.Serializable;

public class IrMaster implements Serializable, Cloneable{

	private static final long serialVersionUID = 1L;

	String institutionID = "-";
	String institutionName = "-";
	String journalID = "-";
	String articleID = "-";
	String item = "-";
	String publisher = "-";
	String publisherID = "-";
	String platform = "-";
	String authors = "-";
	String publicationDate = "-";
	String articleVersion = "-";
	String doi = "-";
	String proprietaryID = "-";
	String isbn = "-";
	String printISSN = "-";
	String onlineISSN = "-";
	String uri = "-";
	String parentTitle = "-";
	String parentAuthors = "-";
	String parentPublicationDate = "-";
	String parentArticleVersion = "-";
	String parentDataType = "-";
	String parentDOI = "-";
	String parentProprietaryID = "-";
	String parentISBN = "-";
	String parentPrintISSN = "-";
	String parentOnlineISSN = "-";
	String parentURI = "-";
	String componentTitle = "-";
	String componentAuthors = "-";
	String componentArticleVersion = "-";
	String componentDataType = "-";
	String componentDOI = "-";
	String componentProprietaryID = "-";
	String componentISBN = "-";
	String componentPrintISSN = "-";
	String componentOnlineISSN = "-";
	String componentURI = "-";
	String dataType = "-";
	String sectionType = "-";
	String yop = "0";
	String accessType = "Controlled";
	String accessMethod = "Regular";
	String metricType = "Search_Automated";
	private int reportingPeriodTotal;
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
	public String getArticleID() {
		return articleID;
	}
	public void setArticleID(String articleID) {
		this.articleID = articleID;
	}
	public String getItem() {
		return item;
	}
	public void setItem(String item) {
		this.item = item;
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
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getAuthors() {
		return authors;
	}
	public void setAuthors(String authors) {
		this.authors = authors;
	}
	public String getPublicationDate() {
		return publicationDate;
	}
	public void setPublicationDate(String publicationDate) {
		this.publicationDate = publicationDate;
	}
	public String getArticleVersion() {
		return articleVersion;
	}
	public void setArticleVersion(String articleVersion) {
		this.articleVersion = articleVersion;
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
	public String getIsbn() {
		return isbn;
	}
	public void setIsbn(String isbn) {
		this.isbn = isbn;
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
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getParentTitle() {
		return parentTitle;
	}
	public void setParentTitle(String parentTitle) {
		this.parentTitle = parentTitle;
	}
	public String getParentAuthors() {
		return parentAuthors;
	}
	public void setParentAuthors(String parentAuthors) {
		this.parentAuthors = parentAuthors;
	}
	public String getParentPublicationDate() {
		return parentPublicationDate;
	}
	public void setParentPublicationDate(String parentPublicationDate) {
		this.parentPublicationDate = parentPublicationDate;
	}
	public String getParentArticleVersion() {
		return parentArticleVersion;
	}
	public void setParentArticleVersion(String parentArticleVersion) {
		this.parentArticleVersion = parentArticleVersion;
	}
	public String getParentDataType() {
		return parentDataType;
	}
	public void setParentDataType(String parentDataType) {
		this.parentDataType = parentDataType;
	}
	public String getParentDOI() {
		return parentDOI;
	}
	public void setParentDOI(String parentDOI) {
		this.parentDOI = parentDOI;
	}
	public String getParentProprietaryID() {
		return parentProprietaryID;
	}
	public void setParentProprietaryID(String parentProprietaryID) {
		this.parentProprietaryID = parentProprietaryID;
	}
	public String getParentISBN() {
		return parentISBN;
	}
	public void setParentISBN(String parentISBN) {
		this.parentISBN = parentISBN;
	}
	public String getParentPrintISSN() {
		return parentPrintISSN;
	}
	public void setParentPrintISSN(String parentPrintISSN) {
		this.parentPrintISSN = parentPrintISSN;
	}
	public String getParentOnlineISSN() {
		return parentOnlineISSN;
	}
	public void setParentOnlineISSN(String parentOnlineISSN) {
		this.parentOnlineISSN = parentOnlineISSN;
	}
	public String getParentURI() {
		return parentURI;
	}
	public void setParentURI(String parentURI) {
		this.parentURI = parentURI;
	}
	public String getComponentTitle() {
		return componentTitle;
	}
	public void setComponentTitle(String componentTitle) {
		this.componentTitle = componentTitle;
	}
	public String getComponentAuthors() {
		return componentAuthors;
	}
	public void setComponentAuthors(String componentAuthors) {
		this.componentAuthors = componentAuthors;
	}
	public String getComponentArticleVersion() {
		return componentArticleVersion;
	}
	public void setComponentArticleVersion(String componentArticleVersion) {
		this.componentArticleVersion = componentArticleVersion;
	}
	public String getComponentDataType() {
		return componentDataType;
	}
	public void setComponentDataType(String componentDataType) {
		this.componentDataType = componentDataType;
	}
	public String getComponentDOI() {
		return componentDOI;
	}
	public void setComponentDOI(String componentDOI) {
		this.componentDOI = componentDOI;
	}
	public String getComponentProprietaryID() {
		return componentProprietaryID;
	}
	public void setComponentProprietaryID(String componentProprietaryID) {
		this.componentProprietaryID = componentProprietaryID;
	}
	public String getComponentISBN() {
		return componentISBN;
	}
	public void setComponentISBN(String componentISBN) {
		this.componentISBN = componentISBN;
	}
	public String getComponentPrintISSN() {
		return componentPrintISSN;
	}
	public void setComponentPrintISSN(String componentPrintISSN) {
		this.componentPrintISSN = componentPrintISSN;
	}
	public String getComponentOnlineISSN() {
		return componentOnlineISSN;
	}
	public void setComponentOnlineISSN(String componentOnlineISSN) {
		this.componentOnlineISSN = componentOnlineISSN;
	}
	public String getComponentURI() {
		return componentURI;
	}
	public void setComponentURI(String componentURI) {
		this.componentURI = componentURI;
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
	public int getReportingPeriodTotal() {
		return reportingPeriodTotal;
	}
	public void setReportingPeriodTotal(int reportingPeriodTotal) {
		this.reportingPeriodTotal = reportingPeriodTotal;
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
	
}
