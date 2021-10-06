package com.mps.enums;

import java.io.Serializable;

public enum MongoConfig implements Serializable {
	DATE("date"), 
	NAME("name"), 
	STATUS("status"),
	//stats status
	VS_STATS_DAILY_TO_DAILY("vs_stats_daily_to_daily"), 
	VS_STATS_DAILY_TO_MONTHLY("vs_stats_daily_to_monthly"), 
	VS_STATS_DAILY_TO_YEARLY("vs_stats_daily_to_yearly"),
	VS_STATS_MONTHLY_TO_DAILY("vs_stats_monthly_to_daily"), 
	VS_STATS_MONTHLY_TO_MONTHLY("vs_stats_monthly_to_monthly"), 
	HS_STATS_MONTHLY("hs_stats_monthly"), 
	HS_STATS_YEARLY("hs_stats_yearly"), 
	HS_IP_STATS_MONTHLY("hs_ip_stats_monthly"), 
	TR_REPORT_STATS("tr_report_stats"),
	IR_REPORT_STATS("ir_report_stats"),
	IR_REPORT_STATS_GROUPBY("ir_report_stats_groupby"),
	PR_REPORT_STATS("pr_report_stats"),
	TR_TO_MYSQL("tr_to_mysql"),
	IR_TO_MYSQL("ir_to_mysql"),
	DR_TO_MYSQL("dr_to_mysql"),
	PR_TO_MYSQL("pr_to_mysql"),
	
	COUNTER_COLLECTION("counter_collection"),
	TR_REPORT_STATS_COLLECTION("tr_report_stats_collection"),
	IR_REPORT_STATS_COLLECTION("ir_report_stats_collection"),
	IR_REPORT_STATS_COLLECTION_GROUPBY("ir_report_stats_collection_groupby"),
	PR_REPORT_STATS_COLLECTION("pr_report_stats_collection"),
	STATISTCS("statistics"),
	STATE("state"),
	//client details
	CLIENT_ID("client_id"),
	CLIENT_CODE("client_code"),
	
	REMARKS("remarks"),
	DESCRIPTION("description"),
	
	EXISTING_STATS_COLLECTION_ACTION("existing_stats_collection_action"),
	//uri
	COUNTER_URI("counter_uri"),
	STATS_URI("stats_uri"),
	FEEDER_URI("feeder_uri"),
	//database
	COUNTER_DATABASE("counter_database"),
	STATS_DATABASE("stats_database"),
	FEEDER_DATABASE("feeder_database"),
	//collections
	FEED_JOURNALS("feed_journals"),
	PAGE_METRIC("page_metric"),
	ACCOUNTS("accounts"),
	ARTICLE("articles"),
	//MySql details
	MYSQL_HOST("mysql_host"),
	MYSQL_PORT("mysql_port"),
	MYSQL_USER("mysql_user"),
	MYSQL_PASSWORD("mysql_password"),
	MYSQL_DATABASE("mysql_database"),
	TR_TABLE_NAME("tr_table_name"),
	IR_TABLE_NAME("ir_table_name"),
	DR_TABLE_NAME("dr_table_name"),
	PR_TABLE_NAME("pr_table_name"),
	
	MONGO_HOST("mongo_host"),
	MONGO_DATABASE("mongo_database"),
	MONGO_COLLECTION("mongo_collection");

	private String value;

	private MongoConfig(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
