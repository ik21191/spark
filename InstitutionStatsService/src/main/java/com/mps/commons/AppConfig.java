package com.mps.commons;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;

import com.mps.utils.Methods;
import com.mps.utils.MyLogger;

public class AppConfig implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private static Map<String, String> propertyFileMap = new HashMap<>();
	private static Map<String, String> mongoConfigMap = new HashMap<>();
	private static Document currentProcess;
	
	public static String get(String key)throws Exception {
		String value = null;
		try {
			if((value = propertyFileMap.get(key)) != null) {
				return value;
			} else if((value = mongoConfigMap.get(key)) != null) {
				return value;
			} else {
				throw new NullPointerException();
			}
		} catch(Exception e) {
			MyLogger.error("AppConfig: Property not found for the key: " + key);
			throw e;
		}
	}
	
	public static void initialize() throws Exception {
		try {
			propertyFileMap.clear();
			mongoConfigMap.clear();
			readPropertyFile();
		} catch(Exception e) {
			MyLogger.error("AppConfig: initialize(): EXCEPTION: " + e);
			throw e;
		}
	}
	
	private static void readPropertyFile() throws Exception {
		try {
			Methods methods = new Methods("ksv-spark");
			String rootPath = methods.getClassPath(methods);
            String configPath = rootPath + File.separator + "config" + File.separator + "config.mps";
            propertyFileMap = methods.readPropertyFile(configPath);
            displayPropertyFile(propertyFileMap);
		} catch(Exception e) {
			MyLogger.error("AppConfig: readPropertyFile(): EXCEPTION: " + e);
			throw e;
		}
	}
	
	/**Runs recursively to populate values*/
	private static void readFromMongoConfig(Document document) throws Exception {
		try {
			for(Entry<String, Object> entry : document.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				if(value instanceof Document) {
					readFromMongoConfig((Document)value);
				}
				
				if(!(value instanceof Document)) {
					mongoConfigMap.put(key, value.toString());
				}
			}
		} catch(Exception e) {
			MyLogger.error("AppConfig: readFromMongoConfig(): EXCEPTION: " + e);
			throw e;
		}
	}
	public static void initializeMongoConfig(Document document) throws Exception {
		mongoConfigMap.clear();
		readFromMongoConfig(document);
		displayPropertyFile(mongoConfigMap);
	}
	
	private static void displayPropertyFile(Map<String, String> map) {
		MyLogger.log("AppConfig: displayPropertyFile: Start");
		for(Entry<String, String> entry : map.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			MyLogger.log(key + "=" + value);
		}
		MyLogger.log("AppConfig: displayPropertyFile: End");
	}

	public static Document getCurrentProcess() {
		return currentProcess;
	}

	public static void setCurrentProcess(Document currentProcess) {
		AppConfig.currentProcess = currentProcess;
	}

}
