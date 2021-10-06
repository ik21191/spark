package com.mps.feeder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mps.utils.MyException;
import com.mps.utils.MyLogger;

import scala.Tuple2;

/**
 * 
 * @author kapil.kumar 201800124
 */
public class RDDGeneric {

	private static JavaSparkContext jsc = null;
	private static String mongoUri = "";
	private static String collection = "";
	private static String database = "";
	private static JavaMongoRDD<Document> masterRDD = null;
	private static long masterRDDCount = 0;

	public static enum KEY_CASE {
		LOWER {
			public String toString() {
				return "LOWER_CASE";
			}
		},

		UPPER {
			public String toString() {
				return "UPPER_CASE";
			}
		},
	}

	/**
	 * initialize() for connection with mongo DB Load collection from Mongo as
	 * java RDD
	 * 
	 * @param sparkJsc
	 *            {initialize spark for Spark-Mongo connection}
	 * @param uri
	 *            {Mongo location URI}
	 * @param db
	 *            {Mongo Database Name}
	 * @param coll
	 *            {Mongo Collection Name}
	 * @throws Exception
	 */
	public static void initialize(JavaSparkContext sparkJsc, String uri, String db, String coll) throws Exception {
		HashMap<String, String> readOverrides = new HashMap<String, String>();
		ReadConfig readConfig = null;
		double iTracker = 0.0;
		try {
			iTracker = 1.0;
			// Check for java spark context
			if (sparkJsc == null) {
				throw new MyException("Invalid Java Spark Context : " + sparkJsc);
			}
			// Check for URI
			if (uri == null || uri.equalsIgnoreCase("null") || uri.equalsIgnoreCase("") || uri.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Uri : " + uri);
			}
			// Check for Database
			if (db == null || uri.equalsIgnoreCase("null") || db.equalsIgnoreCase("") || db.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Database : " + db);
			}
			// Check for Collection
			if (coll == null || coll.equalsIgnoreCase("null") || coll.equalsIgnoreCase("")
					|| coll.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Collection : " + coll);
			}
			//
			iTracker = 2.0;
			jsc = sparkJsc;
			mongoUri = uri;
			database = db;
			collection = coll;
			// Check for RDD is Empty
			iTracker = 3.0;
			/* if(masterRDD == null){ */
			// MyLogger.log("RDDGenric : initialize() : Start");
			iTracker = 3.1;
			readOverrides.put("spark.mongodb.input.uri", mongoUri);
			readOverrides.put("database", database);
			readOverrides.put("collection", collection);
			iTracker = 3.2;
			readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
			// MyLogger.log("ReadConfig : readOverrides : "+
			// readOverrides.toString());

			iTracker = 3.3;
			masterRDD = MongoSpark.load(jsc, readConfig);
			/*masterRDDCount = masterRDD.count();
			ReportGenerator.setMasterDataRDD_count(masterRDDCount);
			iTracker = 3.4;
			// MyLogger.log("MongoSpark.load(jsc,readConfig) : " +
			// masterRDDCount + " : Records Found");

			if (masterRDDCount <= 0) {
				iTracker = 3.5;
				throw new Exception("No Record found in : " + mongoUri + " : " + database + " : " + collection);
			}*/
			iTracker = 3.6;
			/*
			 * }else{ MyLogger.
			 * log("RDDGenric : initialize() : RDD Already Initailized : " +
			 * masterRDDCount + " : Records Found"); }
			 */

		} catch (Exception e) {
			throw new MyException("RDDGenric : initialize() : iTracker : " + iTracker + " : " + e.toString());
		}
		// MyLogger.log("RDDGenric : initialize() : End");
	}

	/// future look up
	public static void setMongoRDD(JavaSparkContext sparkJsc, String uri, String db, String coll) throws Exception {
		HashMap<String, String> readOverrides = new HashMap<String, String>();
		ReadConfig readConfig = null;
		long dataRDDCount = 0;
		double iTracker = 0.0;
		try {
			iTracker = 1.0;
			// Check for java spark context
			if (sparkJsc == null) {
				throw new MyException("Invalid Java Spark Context : " + sparkJsc);
			}
			// Check for URI
			if (uri == null || uri.equalsIgnoreCase("null") || uri.equalsIgnoreCase("") || uri.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Uri : " + uri);
			}
			// Check for Database
			if (db == null || uri.equalsIgnoreCase("null") || db.equalsIgnoreCase("") || db.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Database : " + db);
			}
			// Check for Collection
			if (coll == null || coll.equalsIgnoreCase("null") || coll.equalsIgnoreCase("")
					|| coll.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Collection : " + coll);
			}
			//
			iTracker = 2.0;
			jsc = sparkJsc;
			mongoUri = uri;
			database = db;
			collection = coll;
			// Check for RDD is Empty
			iTracker = 3.0;
			/* if(masterRDD == null){ */
			// MyLogger.log("RDDGenric : initialize() : Start");
			iTracker = 3.1;
			readOverrides.put("spark.mongodb.input.uri", mongoUri);
			readOverrides.put("database", database);
			readOverrides.put("collection", collection);
			iTracker = 3.2;
			readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
			// MyLogger.log("ReadConfig : readOverrides : "+
			// readOverrides.toString());

			iTracker = 3.3;
			masterRDD = MongoSpark.load(jsc, readConfig);
			/*masterRDDCount = masterRDD.count();

			iTracker = 3.4;
			// MyLogger.log("MongoSpark.load(jsc,readConfig) : " +
			// masterRDDCount + " : Records Found");

			if (dataRDDCount <= 0) {
				iTracker = 3.5;
				throw new Exception("No Record found in : " + mongoUri + " : " + database + " : " + collection);
			}*/
			iTracker = 3.6;
			/*
			 * }else{ MyLogger.
			 * log("RDDGenric : initialize() : RDD Already Initailized : " +
			 * masterRDDCount + " : Records Found"); }
			 */

		} catch (Exception e) {
			throw new MyException("RDDGenric : initialize() : iTracker : " + iTracker + " : " + e.toString());
		}
		// MyLogger.log("RDDGenric : initialize() : End");
	}

	public static JavaMongoRDD<Document> getMongoRDD(JavaSparkContext sparkJsc, String uri, String db, String coll)
			throws Exception {
		HashMap<String, String> readOverrides = new HashMap<String, String>();
		ReadConfig readConfig = null;
		JavaMongoRDD<Document> dataRDD = null;
		long dataRDDCount = 0;
		double iTracker = 0.0;
		try {
			iTracker = 1.0;
			// Check for java spark context
			if (sparkJsc == null) {
				throw new MyException("Invalid Java Spark Context : " + sparkJsc);
			}
			// Check for URI
			if (uri == null || uri.equalsIgnoreCase("null") || uri.equalsIgnoreCase("") || uri.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Uri : " + uri);
			}
			// Check for Database
			if (db == null || uri.equalsIgnoreCase("null") || db.equalsIgnoreCase("") || db.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Database : " + db);
			}
			// Check for Collection
			if (coll == null || coll.equalsIgnoreCase("null") || coll.equalsIgnoreCase("")
					|| coll.equalsIgnoreCase("-")) {
				throw new MyException("Invalid Mongo Collection : " + coll);
			}
			//
			iTracker = 2.0;
			jsc = sparkJsc;
			mongoUri = uri;
			database = db;
			collection = coll;
			// Check for RDD is Empty
			iTracker = 3.0;
			/* if(masterRDD == null){ */
			// MyLogger.log("RDDGenric : initialize() : Start");
			iTracker = 3.1;
			readOverrides.put("spark.mongodb.input.uri", mongoUri);
			readOverrides.put("database", database);
			readOverrides.put("collection", collection);
			iTracker = 3.2;
			readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
			// MyLogger.log("ReadConfig : readOverrides : "+
			// readOverrides.toString());

			iTracker = 3.3;
			dataRDD = MongoSpark.load(jsc, readConfig);
			/*dataRDDCount = dataRDD.count();
			ReportGenerator.setDataRDDCount_count(dataRDDCount);
			iTracker = 3.4;
			// MyLogger.log("MongoSpark.load(jsc,readConfig) : " +
			// masterRDDCount + " : Records Found");

			if (dataRDDCount <= 0) {
				iTracker = 3.5;
				throw new Exception("No Record found in : " + mongoUri + " : " + database + " : " + collection);
			}*/
			iTracker = 3.6;
			/*
			 * }else{ MyLogger.
			 * log("RDDGenric : initialize() : RDD Already Initailized : " +
			 * masterRDDCount + " : Records Found"); }
			 */

		} catch (Exception e) {
			throw new MyException("RDDGenric : initialize() : iTracker : " + iTracker + " : " + e.toString());
		}
		return dataRDD;
		// MyLogger.log("RDDGenric : initialize() : End");
	}

	/*
	 * Getting RDD and its Type 1. Get Simple RDD without filter 2. Get Simple
	 * RDD with Filter 3. Get Simple RDD as List without Filter 4. Get Simple
	 * RDD as List with Filter 5. Get Paired RDD without Filter 6. Get Paired
	 * RDD with Filter 7. Get Paired RDD as Map without Filter 8. Get Paired RDD
	 * as Map with Filter 9. Get Paired RDD as List without Filter 10. Get
	 * Paired RDD as List with Filter
	 */

	/**
	 * Get Mongo collection as javaRDD
	 * 
	 * @return JavaRDD<Document>
	 * @throws Exception
	 */
	public static JavaRDD<Document> getRDD() throws Exception {
		Double iTracker = 0.0;
		try {
			iTracker = 1.0;
			/*if (masterRDDCount < 1) {
				throw new MyException("No Record found in : " + mongoUri + " : " + database + " : " + collection);
			}*/
		} catch (Exception e) {
			MyLogger.exception("RDDGenric : getRDD() : iTracker : " + iTracker + " : " + e.getMessage());
		}
		return masterRDD;
	}

	/**
	 * Get List of RDD Document without applying any filter
	 * 
	 * @return List<Document>
	 * @throws Exception
	 */
	public static List<Document> getRDDList() throws Exception {
		List<Document> rddList = null;
		Double iTracker = 0.0;
		try {
			iTracker = 1.0;
			/*if (masterRDDCount < 1) {
				throw new MyException("No Record found in : " + mongoUri + " : " + database + " : " + collection);
			} else {
				rddList = masterRDD.collect();
			}*/
			if (masterRDDCount >= 1) {
				rddList = masterRDD.collect();
			}

		} catch (Exception e) {
			MyLogger.exception("RDDGenric : getRDDList() : iTracker : " + iTracker + " : " + e.getMessage());
		}
		return rddList;
	}

	/**
	 * Get filtered Java RDD
	 * 
	 * @param filterMap
	 *            {HashMap<String, String> for filtering RDD with passed map
	 *            attributes}
	 * @return JavaRDD<Document>
	 * @throws Exception
	 */
	public static JavaRDD<Document> getFilteredRDD(final HashMap<String, String> filterMap) throws Exception {
		Double iTracker = 0.0;
		JavaRDD<Document> jRDDFilter = null;
		long count = 0l;
		try {
			iTracker = 1.0;
			/*if (masterRDDCount < 1) {
				throw new MyException("No Record found in : " + mongoUri + " : " + database + " : " + collection);
			}*/

			// MyLogger.log("RDDGenric : getFilteredRDD() : Start");
			iTracker = 2.0;
			jRDDFilter = masterRDD.filter(doc -> filterDoc(doc, filterMap));
//			count = jRDDFilter.count();
			// MyLogger.log("MongoSpark.load(jsc,readConfig) : " + count + " :
			// Filtered Records Found");

		} catch (Exception e) {
			MyLogger.exception("RDDGenric : getFilteredRDD() : record found : " + count + " : iTracker : " + iTracker
					+ " : " + e.getMessage());
		}

		return jRDDFilter;

	}

	/**
	 * Get Filtered List of RDD Document
	 * 
	 * @param filterMap
	 *            {HashMap<String, String> for filtering RDD with passed map
	 *            attributes}
	 * @return List<Document>
	 * @throws Exception
	 */
	public static List<Document> getFilteredRDDList(final HashMap<String, String> filterMap) throws Exception {
		Double iTracker = 0.0;
		JavaRDD<Document> jRDDFilter = null;
		List<Document> jRDDFilterList = null;
		try {
			iTracker = 1.0;
			/*if (masterRDDCount < 1) {
				throw new MyException("No Record found in : " + mongoUri + " : " + database + " : " + collection);
			}*/
			// Filtered RDD Method Called
			iTracker = 2.0;
			jRDDFilter = getFilteredRDD(filterMap);
			iTracker = 3.0;
			jRDDFilterList = jRDDFilter.collect();

		} catch (Exception e) {
			MyLogger.exception("RDDGenric : getFilteredRDDList() : iTracker : " + iTracker + " : " + e.getMessage());
		}
		return jRDDFilterList;
	}

	// =================================Getting RDD with Primary Key
	// ==========================
	/**
	 * Key value pair RDD Without Filter, key is in lower case, and not case
	 * sensitive
	 * 
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @return JavaPairRDD<String, Document>
	 * @throws Exception
	 */
	public static JavaPairRDD<String, Document> getPairedRDD(final String key) throws Exception {
		return getPairedRDD(key, null);
	}

	/**
	 * Key value pair RDD Without Filter, key is in lower case, and not case
	 * sensitive {if passed parameter is false} key is in original format, with
	 * case sensitive {if passed parameter is true}
	 * 
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @param caseSensitive
	 *            {if ture, key will be case sensitive, if fase, key will be in
	 *            lower case}
	 * @return JavaPairRDD<String, Document>
	 * @throws Exception
	 */

	// generic key in lower case and value pair RDD Without Filter (Key in lower
	// case)
	public static JavaPairRDD<String, Document> getPairedRDD(final String key, KEY_CASE keyCase) throws Exception {
		JavaRDD<Document> outputRDD = null;
		JavaPairRDD<String, Document> pairedRDD = null;
		Long count = 0l;
		Double iTracker = 0.0;
		try {
			iTracker = 1.0;
			outputRDD = getRDD();

			iTracker = 2.0;
			pairedRDD = outputRDD.mapToPair(new PairFunction<Document, String, Document>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Document> call(Document doc) throws Exception {
					String keyValue = doc.get(key).toString().trim();
					if (keyCase != null) {
						if (keyCase.toString().trim().equalsIgnoreCase("LOWER_CASE")) {
							keyValue = keyValue.toLowerCase();
						} else if (keyCase.toString().trim().equalsIgnoreCase("UPPER_CASE")) {
							keyValue = keyValue.toUpperCase();
						}
					}
					return new Tuple2<String, Document>(keyValue, doc);
				}
			});
//			count = pairedRDD.count();
			// MyLogger.log("RDDGenric : getPairedRDD() : End : " +
			// pairedRDD.count() + " : Records Found");
		} catch (Exception e) {
			throw new MyException("RDDGenric : getPairedRDD() : records found : " + count + " : iTracker : " + iTracker
					+ " : " + e.toString());
		}

		return pairedRDD;
	}

	/**
	 * Key value pair RDD with Filter, key is in lower case, and not case
	 * sensitive
	 * 
	 * @param filterMap
	 *            {HashMap <String, String>, filter RDD with map attributes}
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @return JavaPairRDD<String, Document>
	 * @throws Exception
	 */

	// generic filter and with key value pair with filter
	public static JavaPairRDD<String, Document> getFilteredPairedRDD(final HashMap<String, String> filterMap,
			final String key) throws Exception {
		return getFilteredPairedRDD(filterMap, key, null);
	}

	/**
	 * Key value pair RDD with Filter, key is in lower case, and not case
	 * sensitive {if passed parameter is false} key is in original format, with
	 * case sensitive {if passed parameter is true}
	 * 
	 * @param filterMap
	 *            {HashMap <String, String>, filter RDD with map attributes}
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @param caseSensitive
	 *            {if ture, key will be case sensitive, if fase, key will be in
	 *            lower case}
	 * @return JavaPairRDD<String, Document>
	 * @throws Exception
	 */
	public static JavaPairRDD<String, Document> getFilteredPairedRDD(final HashMap<String, String> filterMap,
			final String key, KEY_CASE keyCase) throws Exception {
		JavaRDD<Document> outputRDD = null;
		JavaPairRDD<String, Document> pairedRDD = null;
		Long count = 0l;
		Double iTracker = 0.0;
		try {
			iTracker = 1.0;
			outputRDD = getFilteredRDD(filterMap);

			iTracker = 2.0;
			pairedRDD = outputRDD.mapToPair(new PairFunction<Document, String, Document>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Document> call(Document doc) throws Exception {
					String keyValue = doc.get(key).toString().trim();
					if (keyCase != null) {
						if (keyCase.toString().trim().equalsIgnoreCase("LOWER_CASE")) {
							keyValue = keyValue.toLowerCase();
						} else if (keyCase.toString().trim().equalsIgnoreCase("UPPER_CASE")) {
							keyValue = keyValue.toUpperCase();
						}
					}
					return new Tuple2<String, Document>(keyValue, doc);
				}
			});

//			count = pairedRDD.count();
			// MyLogger.log("RDDGenric : getFilteredPairedRDD() : End : " +
			// pairedRDD.count() + " : Records Found");
		} catch (Exception e) {
			throw new MyException("RDDGenric : getFilteredPairedRDD() : record found : " + count + " : iTracker : "
					+ iTracker + " : " + e.toString());
		}
		return pairedRDD;
	}

	/**
	 * Key value paired RDD as Map Without Filter, key is in lower case, and not
	 * case sensitive
	 * 
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @return Map<String, Document>
	 * @throws Exception
	 */
	public static Map<String, Document> getPairedRDDMap(final String key) throws Exception {

		return getPairedRDD(key).collectAsMap();
	}

	/**
	 * Key value pair RDD as Map without filter, key is in lower case, and not
	 * case sensitive {if passed parameter is false} key is in original format,
	 * with case sensitive {if passed parameter is true}
	 * 
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @param caseSensitive
	 *            {if ture, key will be case sensitive, if fase, key will be in
	 *            lower case}
	 * @return Map<String, Document>
	 * @throws Exception
	 */
	public static Map<String, Document> getPairedRDDMap(final String key, KEY_CASE keyCase) throws Exception {

		return getPairedRDD(key, keyCase).collectAsMap();
	}

	/**
	 * Key value pair RDD as Map with filter, key is in lower case, and not case
	 * sensitive
	 * 
	 * @param filterMap
	 *            {HashMap <String, String>, filter RDD with map attributes}
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @return Map<String, Document>
	 * @throws Exception
	 */
	public static Map<String, Document> getFilteredPairedRDDMap(final HashMap<String, String> filterMap,
			final String key) throws Exception {

		return getFilteredPairedRDD(filterMap, key).collectAsMap();
	}

	/**
	 * Key value pair RDD as Map with filter, key is in lower case, and not case
	 * sensitive {if passed parameter is false} key is in original format, with
	 * case sensitive {if passed parameter is true}
	 * 
	 * @param filterMap
	 *            {HashMap <String, String>, filter RDD with map attributes}
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @param keyCase
	 *            {if ture, key will be case sensitive, if fase, key will be in
	 *            lower case}
	 * @return Map<String, Document>
	 * @throws Exception
	 */
	public static Map<String, Document> getFilteredPairedRDDMap(final HashMap<String, String> filterMap,
			final String key, KEY_CASE keyCase) throws Exception {

		return getFilteredPairedRDD(filterMap, key, keyCase).collectAsMap();
	}

	/**
	 * Tuple with Key value pair RDD as List without filter, key is in lower
	 * case, and not case sensitive
	 * 
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @return List {Tuple2(String,Document)}
	 * @throws Exception
	 */
	public static List<Tuple2<String, Document>> getPairedRDDList(final String key) throws Exception {

		return getPairedRDD(key).collect();
	}

	/**
	 * Tuple with Key value pair RDD as List without filter, key is in lower
	 * case, and not case sensitive {if passed parameter is false} key is in
	 * original format, with case sensitive {if passed parameter is true}
	 * 
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @param caseSensitive
	 *            {if ture, key will be case sensitive, if fase, key will be in
	 *            lower case}
	 * @return List {Tuple2(String,Document)}
	 * @throws Exception
	 */

	// generic RDD as List without filter (Key in lower case)
	public static List<Tuple2<String, Document>> getPairedRDDList(final String key, KEY_CASE keyCase) throws Exception {

		return getPairedRDD(key, keyCase).collect();
	}

	/**
	 * Tuple with Key value pair RDD as List with filter, key is in lower case,
	 * and not case sensitive
	 * 
	 * @param filterMap
	 *            {HashMap <String, String>, filter RDD with map attributes}
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @return List {Tuple2(String,Document)}
	 * @throws Exception
	 */

	// generic RDD as List with filter
	public static List<Tuple2<String, Document>> getFilteredPairedRDDList(final HashMap<String, String> filterMap,
			final String key) throws Exception {

		return getFilteredPairedRDD(filterMap, key).collect();
	}

	/**
	 * Tuple with Key value pair RDD as List with filter, key is in lower case,
	 * and not case sensitive {if passed parameter is false} key is in original
	 * format, with case sensitive {if passed parameter is true}
	 * 
	 * @param filterMap
	 *            {HashMap <String, String>, filter RDD with map attributes}
	 * @param key
	 *            {name of parameter, will be key for Document}
	 * @param caseSensitive
	 *            {if ture, key will be case sensitive, if fase, key will be in
	 *            lower case}
	 * @return List {Tuple2(String,Document)}
	 * @throws Exception
	 */

	// generic RDD as List with filter (Key in lower case)
	public static List<Tuple2<String, Document>> getFilteredPairedRDDList(final HashMap<String, String> filterMap,
			final String key, KEY_CASE keyCase) throws Exception {

		return getFilteredPairedRDD(filterMap, key, keyCase).collect();
	}

	// =================================Implemented Method Called From other
	// Methods ==========================
	// Method Implemented for Filter RDD as per Parmeters
	private static boolean filterDoc(Document doc, final HashMap<String, String> filterMap) {

		String key = null;
		String value = null;
		String docValue = null;
		boolean continueFlag = false;

		try {
			for (Entry<String, String> entry : filterMap.entrySet()) {
				continueFlag = false;

				key = entry.getKey();
				if (key != null) {
					key = key.trim();
				}
				if (doc.containsKey(key)) {
					docValue = doc.get(key).toString();
					value = entry.getValue();
					if(key.equalsIgnoreCase("collection_type")){
						if(value.trim().equalsIgnoreCase("Any")){
							if (docValue.trim().equalsIgnoreCase("Journal") || docValue.trim().equalsIgnoreCase("Book")) {
								continueFlag = true;
							} else {
								break;
							}
						}	
					}else{
						if (docValue.trim().equalsIgnoreCase(value.trim())) {
							continueFlag = true;
						} else {
							break;
						}	
					}
					
					
					
				} else {
					break;
				}

			}
		} catch (Exception e) {
			// what to do
		}
		return continueFlag;
	}

	public static void destroy() {

	}

}
