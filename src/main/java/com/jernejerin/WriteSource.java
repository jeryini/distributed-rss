/**
 * 
 */
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
 * @author Jernej Jerin
 * 
 * Writes RSS sources from csv file to MongoDB.
 *
 */
public class WriteSource {

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
			
			br = new BufferedReader(new FileReader(args[0]));
		
			String line;
			while ((line = br.readLine()) != null) {
				// save each RSS source into django
				// we also need to set used parameter to 0
				BasicDBObject feedNew = new BasicDBObject("feedUrl", line).append("used", 0);
				rssColl.insert(feedNew);
				break;
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
