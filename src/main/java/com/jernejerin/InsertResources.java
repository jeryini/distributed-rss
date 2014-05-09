package com.jernejerin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * The InsertResources program writes all RSS sources i.e. feeds from csv file
 * to MongoDB.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
public class InsertResources {

	// logger for this class
	static Logger logger = Logger.getLogger(InsertResources.class);

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
			logger.info("Opened connection to MongoDB");

			// purge DB
			rssColl.remove(new BasicDBObject());

			br = new BufferedReader(new FileReader(args[0]));

			String line;
			while ((line = br.readLine()) != null) {
				// save each feed into DB. We also need to set
				// used parameter to 0, as this will be used to
				// check which feeds are in use
				BasicDBObject feedNew = new BasicDBObject("feedUrl", line)
						.append("used", 0);
				rssColl.insert(feedNew);
				logger.info("Inserted new feed into DB.");
			}

			br.close();
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
			logger.info("Closed connection to MongoDB");
		}
	}
}
