package com.mps.start;

import com.mps.commons.AppConfig;
import com.mps.utils.MyLogger;

public class AppLauncher {

	public static void main(String[] args) throws Exception {
		MyLogger.setUser("Inst-Stats-Service");
		try {
			AppConfig.initialize();
			new AppLauncher().run(); 
		} catch (Exception e) {
			throw e;
		}
	}
	
	private void run() throws Exception{
		String processor = "";
		try {
			MyLogger.log("Launching Application....");
			MyLogger.log("Processor Invoked : " + processor);
			Start start = new Start();
			start.run();
		} catch (Exception e) {
			MyLogger.error("AppLauncher: run(): EXCEPTION: " + e);
			throw e;
		}
	}
}
