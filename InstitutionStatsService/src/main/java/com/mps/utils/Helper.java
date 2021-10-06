package com.mps.utils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Helper implements Serializable {
	
	private static final long serialVersionUID = 1L;

	public static String getCurrentDateTime() {
		String currentDateTime = "0000-00-00 00:00:00";
		try {
			currentDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		} catch(Exception e) {
			MyLogger.error("Helper : getCurrentDateTime() : EXCEPTION : " + e);
		}
		return currentDateTime;
	}
}
