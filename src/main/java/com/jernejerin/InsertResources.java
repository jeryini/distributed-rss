package com.jernejerin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * The InsertResources program writes all RSS 
 * sources i.e. feeds from csv file to MongoDB.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since   2014-05-06
 */
public class InsertResources {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader br = null;
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			DB rssDB = mongoClient.getDB("rssdb");
			DBCollection rssColl = rssDB.getCollection("feeds");
			
			// purge DB
			rssColl.remove(new BasicDBObject());
			
			br = new BufferedReader(new FileReader(args[0]));
		
			String line;
			while ((line = br.readLine()) != null) {
				// save each feed into DB. We also need to set 
				// used parameter to 0, as this will be used to
				// check which feeds are in use
				BasicDBObject feedNew = new BasicDBObject("feedUrl", line).append("used", 0);
				rssColl.insert(feedNew);
			}
			
			br.close();
		} catch (UnknownHostException e) {
			System.err.println("Problem with database host.");
			e.printStackTrace();
		} catch (MongoException e) {
			System.err.println("General Mongo problem.");
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			mongoClient.close();
		}
	}
}
