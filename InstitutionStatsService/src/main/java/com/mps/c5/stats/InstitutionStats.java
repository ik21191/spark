package com.mps.c5.stats;

import org.bson.Document;

public class InstitutionStats {
	private static Document document;
	
	public static void setInitial() {
		document = new Document();
		document.
		append("v_daily_to_daily_counter", "NA").
		append("v_daily_to_daily_start_time", "NA").
		append("v_daily_to_daily_end_time", "NA").
		
		append("v_daily_to_monthly_counter", "NA").
		append("v_daily_to_monthly_start_time", "NA").
		append("v_daily_to_monthly_end_time", "NA").
		
		append("v_monthly_to_daily_counter", "NA").
		append("v_monthly_to_daily_start_time", "NA").
		append("v_monthly_to_daily_end_time", "NA").
		
		append("v_monthly_to_monthly_counter", "NA").
		append("v_monthly_to_monthly_start_time", "NA").
		append("v_monthly_to_monthly_end_time", "NA");
	}

	public static Document getDocument() {
		return document;
	}
}
