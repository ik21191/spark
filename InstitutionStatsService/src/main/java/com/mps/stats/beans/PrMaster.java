package com.mps.stats.beans;

import java.io.Serializable;

public class PrMaster implements Serializable, Cloneable{

	private static final long serialVersionUID = 1L;

	String institutionID = "-";
	String institutionName = "-";
	String platform = "-";
	String dataType = "-";
	String yop = "0";
	String accessType = "Controlled";
	String metricType = "Other";
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
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
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
