/**
 * 
 */
package com.jernejerin;

import java.net.UnknownHostException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * @author Jernej Jerin
 *
 */
public class RSSDelegate {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MongoClient mongoClient = null;
		try {
			// we only need one instance of class MongoClient
			// even with multiple threads
			mongoClient = new MongoClient( "localhost" , 27017 );
			DB db = mongoClient.getDB("rssdb");
			DBCollection coll = db.getCollection("feeds");
			
			// our query which returns feeds currently not used
			BasicDBObject query = new BasicDBObject("used", 0);
			
			// create maximum of 100 threads
			for (int i = 0; i < 100; i++) {
				// get the first RSS feed that is currently not used
				DBObject feed = coll.findOne(query);
				
				if (feed != null) {
					String url = (String) feed.get("uri");
					
					// set the feed to used and last access time 
					// and update it
					feed.put("used", 1);
					feed.put("accessedAt", new Date());
					coll.update(new BasicDBObject("uri", url), feed);
					
					// start thread for given RSS url
					new Thread(new RSSReader(url)).start();
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
