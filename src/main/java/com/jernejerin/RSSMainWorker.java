package com.jernejerin;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * This worker represent single virtual machine.
 * 
 * @author Jernej Jerin
 *
 */
public class RSSMainWorker {
	// queue size for active threads
	public static final int QUEUE_SIZE = 1000;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		MongoClient mongoClient = null;
		try {
			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient( "localhost" , 27017 );
			DB rssDB = mongoClient.getDB("rssdb");
			DBCollection rssColl = rssDB.getCollection("feeds");
			DBCollection entriesColl = rssDB.getCollection("entries");
			
			// our query which returns feeds currently not used
			BasicDBObject query = new BasicDBObject("used", 0);
			
			// create a thread pool with fixed number of threads
			// the same as using ThreadPoolExecutor with default values
			ThreadPoolExecutor executor = new ThreadPoolExecutor(QUEUE_SIZE, QUEUE_SIZE, 0L, 
					TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
			
			// check for available threads indefinetly
			while (true) {
				// create maximum of specified jobs
				if (executor.getActiveCount() < QUEUE_SIZE) {
					// get the available RSS feed from the message queue
					// get the first RSS feed that is currently not used
					DBObject feed = rssColl.findOne(query);
					
					if (feed != null) {
						String feedUrl = (String) feed.get("feedUrl");
						
						// set the feed to used and update it
						feed.put("used", 1);
						rssColl.update(new BasicDBObject("feedUrl", feedUrl), feed);
						
						// start thread for given RSS feed
						Runnable rssThreadWorker = new RSSThreadWorker(feed, rssColl, entriesColl);
						executor.execute(rssThreadWorker);
					}
				}
			}
		} catch (UnknownHostException e) {
			System.err.println("Problem with database host.");
			e.printStackTrace();
		} catch (MongoException e) {
			System.err.println("General Mongo problem.");
			e.printStackTrace();
		} catch (IllegalThreadStateException e) {
			System.err.println("Problem with threading.");
		} finally {
			mongoClient.close();
		}
	}
}
