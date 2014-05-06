/**
 * 
 */
package com.jernejerin.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * Writes predefined randomly selected RSS sources from csv file to MongoDB.
 * 
 * @author Jernej Jerin
 *
 */
public class InsertTestResources {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader br = null;
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			DB rssDB = mongoClient.getDB("rssdbtest");
			DBCollection rssColl = rssDB.getCollection("feeds");
			DBCollection entriesColl = rssDB.getCollection("entries");
			
			// purge DB
			rssColl.remove(new BasicDBObject());
			entriesColl.remove(new BasicDBObject());
			
			ArrayList<String> feeds = new ArrayList<String>();
			br = new BufferedReader(new FileReader(args[0]));
			int numFeeds = Integer.parseInt(args[1]);
		
			String line;
			while ((line = br.readLine()) != null) {
				// save each RSS source list for further
				// random selection
				feeds.add(line);
			}
			
			// number of specified feeds
			for (int i = 0; i < numFeeds; i++) {
				// select random index given the size of feeds
				int randFeedIndex = (int) (Math.random() * feeds.size());
				String feedUrl = feeds.get(randFeedIndex);
				feeds.remove(randFeedIndex);
				BasicDBObject feedNew = new BasicDBObject("feedUrl", feedUrl).append("used", 0);
				rssColl.insert(feedNew);
			}
			
			br.close();
		} catch (IndexOutOfBoundsException e) {
			System.err.println("Missing argument number of feeds!");
			e.printStackTrace();
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
