package com.jernejerin.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * Writes predefined randomly selected RSS sources from csv file to test
 * MongoDB.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
public class InsertTestResources {
	// logger for this class
	static Logger logger = Logger.getLogger(InsertTestResources.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		BufferedReader br = null;
		MongoClient mongoClient = null;
		try {
			// configure logger
			props.load(new FileInputStream("log4j.properties"));
			PropertyConfigurator.configure(props);
			
			mongoClient = new MongoClient("localhost", 27017);
			DB rssDB = mongoClient.getDB("rssdb");
			DBCollection rssColl = rssDB.getCollection("feeds");
			DBCollection entriesColl = rssDB.getCollection("entries");
			logger.info("Created connection to MongoDB.");

			// purge DB
			rssColl.remove(new BasicDBObject());
			entriesColl.remove(new BasicDBObject());
			logger.info("Purged the DB.");

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
				BasicDBObject feedNew = new BasicDBObject("feedUrl", feedUrl)
						.append("used", 0);
				rssColl.insert(feedNew);
				logger.info("Inserted new random feed into DB.");
			}

			br.close();
		} catch (IndexOutOfBoundsException e) {
			logger.fatal(e.getMessage());
		} catch (UnknownHostException e) {
			logger.fatal(e.getMessage());
		} catch (MongoException e) {
			logger.fatal(e.getMessage());
		} catch (FileNotFoundException e) {
			logger.fatal(e.getMessage());
		} catch (IOException e) {
			logger.fatal(e.getMessage());
		} finally {
			mongoClient.close();
			logger.info("Closed connection to DB.");
		}
	}
}
