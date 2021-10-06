package com.mps.utils;

public class Constants {

	public static final String MONGO_DATABASE = "database";
	public static final String MONGO_COLLECTION = "collection";
	public static final String MONGO_HOST_URI = "uri";
	public static final String LONG_IP_ADDRESS = "long_ip_address";
	public static final String URL = "url";
	public static final String PAGE_TYPE = "page_type";
	public static final String REQUEST_DATE_TIME = "request_date_time";
	public static final String DOUBLE_CLICK_FLAG_NAME = "DCF";
	public static final String INSTITUTION_DETAILS = "institution_details";
	
	public static final String INSTITUTION_ID = "institution_id";
	public static final String INSTITUTION_CODE = "code";
	public static final String INSTITUTION_NAME = "name";
	public static final String JOURNAL_ID = "journal_id";
	public static final String JOURNAL_TITLE = "journal_title";
	public static final String ARTICLE_ID = "article_id";
	
	//TR reports
	public static final String PUBLISHER = "publisher";
	public static final String PLATFORM = "platform";
	public static final String JOURNAL_DOI = "journal_doi";
	public static final String PRINT_ISSN = "print_issn";
	public static final String ONLINE_ISSN = "online_issn";
	
	
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String SESSION_ID = "session_id";
	public static final String USER_AGENT = "user_agent";
	
	public static final String DATABASE = "database";
	public static final String COLLECTION = "collection";
	
	public static final String MONGO_INPUT_URI = "spark.mongodb.input.uri";
	public static final String MONGO_OUTPUT_URI = "#spark.mongodb.output.uri";
	public static final String PAGE_METRIC_ID = "page_metric_id";
	public static final String ACCESS_METHOD = "access_method";
	public static final String DOUBLE_CLICK_DURATION = "double_click_duration";
	
	//Constants for Institution stats
	public static final String YEAR = "year";
	public static final String MONTH = "month";
	public static final String DAY = "day";
	
	public static final String VERTICAL_STATS_DAILY_TO_DAILY = "vs_stats_daily_to_daily";
	public static final String VERTICAL_STATS_DAILY_TO_MONTHLY = "vs_stats_daily_to_monthly";
	public static final String VERTICAL_STATS_DAILY_TO_YEARLY = "vs_stats_daily_to_yearly";
	
	public static final String VERTICAL_STATS_MONTHLY_TO_DAILY = "vs_stats_monthly_to_daily";
	public static final String VERTICAL_STATS_MONTHLY_TO_MONTHLY = "vs_stats_monthly_to_monthly";
	
	public static final String STATUS_TR_REPORT_STATS = "tr_report_stats";
	public static final String STATUS_IR_REPORT_STATS = "ir_report_stats";
	public static final String STATUS_PR_REPORT_STATS = "pr_report_stats";
	public static final String TR_TO_MYSQL = "tr_to_mysql";
	public static final String IR_TO_MYSQL = "ir_to_mysql";
	public static final String PR_TO_MYSQL = "pr_to_mysql";
	public static final String STATUS_IR_REPORT_STATS_GROUPBY = "ir_report_stats_groupby";
	
	
	//metric types
	public static final String TOTAL_ITEM_INVESTIGATION = "total_item_investigation";
	public static final String UNIQUE_ITEM_INVESTIGATION = "unique_item_investigation";
	public static final String UNIQUE_TITLE_INVESTIGATION = "unique_title_investigation";
	public static final String TOTAL_ITEM_REQUEST = "total_item_request";
	public static final String UNIQUE_ITEM_REQUEST = "unique_item_request";
	public static final String UNIQUE_TITLE_REQUEST = "unique_title_request";
	public static final String SEARCHES_PLATFORM = "searches_platform";
}
