package com.jernejerin.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.aliasi.spell.JaccardDistance;
import com.aliasi.tokenizer.IndoEuropeanTokenizerFactory;
import com.aliasi.tokenizer.TokenizerFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class JaccardSimmilarityWebPage {

	// logger for this class
	static Logger logger = Logger.getLogger(InsertTestResources.class);

	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		Properties props = new Properties();
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

			TokenizerFactory tknFac = IndoEuropeanTokenizerFactory.INSTANCE;
			JaccardDistance jaccard = new JaccardDistance(tknFac);

			// get all feeds
			DBCursor cursor = rssColl.find();
			BasicDBObject queryIdHash;
			DBObject entry1, entry2;
			int numSimilar = 0;
			
			try {
				while (cursor.hasNext()) {
					// now for each feed check all possible combinations between
					// entries that belong to that feed
					DBObject feed = cursor.next();
					
					// get all entries for this feed
					ArrayList<String> idList = (ArrayList<String>) feed.get("entries");
					
					// we need list od id's
					if (idList != null) {
						// all two combinations, this n over 2 combinations!
						for (int i = 0; i < idList.size(); i++) {
							for (int j = i + 1; j < idList.size(); j++) {
								// query for entries with this id
								queryIdHash = new BasicDBObject("idHash", idList.get(i));
								entry1 = entriesColl.findOne(queryIdHash);
								
								queryIdHash = new BasicDBObject("idHash", idList.get(j));
								entry2 = entriesColl.findOne(queryIdHash);
	
								// we need both two entries
								if (entry1 != null && entry2 != null) {
									// check if full content is available
									String fullContent1 = (String) entry1.get("fullContent");
									String fullContent2 = (String) entry2.get("fullContent");
									
									// we need full content of both entries
									if (fullContent1 != null && fullContent2 != null) {
										double proximity = jaccard.proximity(fullContent1, fullContent2);
										if (proximity > 0.98) {
											numSimilar++;
										}
									}
								}
							}
						}
					}
				}
			} finally {
				cursor.close();
			}
			System.out.println("Number of similar: " + numSimilar);
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
