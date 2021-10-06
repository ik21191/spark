package com.mps.mongo;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mps.utils.MyException;
import com.mps.utils.MyLogger;

public class MongoDBActions {

	//method to check if the particular collections exists
  	public static boolean isCollectionExists(String uri, String database, String collection) throws Exception{
  		MongoClient client = null;
      	try {
      		client = new MongoClient(uri.replace("mongodb://", ""));
      		MongoDatabase mongoDB = client.getDatabase(database);
      		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collection);
      		
      		if(mongoCollection.count() > 0){
      			return true;
      		}
      		
      		return false;
      	}catch(Exception e) {
      		throw new MyException("MongoDBActions: isCollectionExists : " + e);
      	} finally {
      		if(client != null){
      			client.close();
      		}
      	}
  	}

    //Truncate Collection
    public long truncateCollection(String hostPort, String database, String collection) throws MyException {
		MongoClient client = null;
		long deletedCount = 0;
    	try {
    		client = new MongoClient(hostPort.replace("mongodb://", ""));
    		MongoDatabase mongoDB = client.getDatabase(database);
    		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collection);
    		
    		//To delete all the documents form the collection
    		DeleteResult deleteResult = mongoCollection.deleteMany(new BasicDBObject());
    		deletedCount = deleteResult.getDeletedCount();
    		MyLogger.log("MongoDBActions: truncateCollection : Success");
    	}catch(Exception e) {
    		throw new MyException("MongoDBActions: truncateCollection : "+ hostPort+ " : " + database + " : " + collection + " : " +  e);
    	} finally {
    		if(client != null){
    			client.close();
    		}
    	}
    	return deletedCount;
	}
    
    //Drop Collection
    public static void dropCollection(String hostPort, String database, String collection) throws MyException {
		MongoClient client = null;
    	try {
    		client = new MongoClient(hostPort.replace("mongodb://", ""));
    		MongoDatabase mongoDB = client.getDatabase(database);
    		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collection);
    		
    		//To Drop collection
    		mongoCollection.drop();
    		MyLogger.log("MongoDBActions: dropCollection : Success");
    	}catch(Exception e) {
    		throw new MyException("MongoDBActions: dropCollection : "+ hostPort+ " : " + database + " : " + collection + " : " +  e);
    	} finally {
    		if(client != null){
    			client.close();
    		}
    	}
	}

    //Rename Collection
    public static void renameCollection(String hostPort, String database, String collection, String newCollection) throws MyException {
		
    	MongoClient client = null;
		MongoNamespace mongoNamespace = null;
    	try {
    		client = new MongoClient(hostPort.replace("mongodb://", ""));
    		MongoDatabase mongoDB = client.getDatabase(database);
    		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collection);
    		
    		//To rename collection
    		mongoNamespace = new MongoNamespace(database, newCollection);
    		mongoCollection.renameCollection(mongoNamespace);
    		MyLogger.log("MongoDBActions: renameCollection: Success");
    	}catch(Exception e) {
    		throw new MyException("MongoDBActions: renameCollection : "+ hostPort+ " : " + database + " : " + collection + " : " + newCollection + " : " + e);
    	} finally {
    		if(client != null){
    			client.close();
    		}
    	}
	}
    
    //method to update taskDoc in task config collections with status and description
	public static void updateDoc(String uri, String database, String collection, Document document) {
    	MongoClient mongoClient = null;
    	try {
    		mongoClient = new MongoClient(uri.replace("mongodb://", ""));
    		MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
    		MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
    		
    		Bson filter = new Document("_id", document.getObjectId("_id"));
    		Bson updateOperationDocument = new Document("$set", document);
    		mongoCollection.updateOne(filter, updateOperationDocument);

    	}catch(Exception e) {
    		MyLogger.exception("MongoDBActions: updateTask()" + e);
    	} finally {
    		if(mongoClient != null) {
    			mongoClient.close();
    		}
    	}
	}
	
	public static void insertDoc(String uri, String database, String collection, Document document) {
		MongoClient client = null;
		try {
			client = new MongoClient(uri.replace("mongodb://", ""));
    		MongoDatabase mongoDB = client.getDatabase(database);
    		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collection);
    		mongoCollection.insertOne(document);
		} catch(Exception e) {
			MyLogger.exception("MongoDBActions: insert: EXCEPTION: " + e);	
		} finally {
			if(client != null) {
				client.close();
			}
		}
	}
	
}
