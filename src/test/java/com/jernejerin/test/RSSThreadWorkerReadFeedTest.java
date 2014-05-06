/**
 * 
 */
package com.jernejerin.test;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.jernejerin.RSSThreadWorker;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.sun.syndication.io.SyndFeedInput;

/**
 * Parametrized test for read feed function.
 * 
 * @author Jernej Jerin
 *
 */
@RunWith(Parameterized.class)
public class RSSThreadWorkerReadFeedTest {
	static MongoClient mongoClient = null;
	static DBCollection rssColl;
	
	private String url;
	private SyndFeedInput input;
	
	public RSSThreadWorkerReadFeedTest(String url, SyndFeedInput input) {
		this.url = url;
		this.input = input;
	}

	/**
	 * @throws java.lang.Exception
	 */
	public static void setUpDatabase() throws Exception {
		try {
			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient( "localhost" , 27017 );
			
			// using test database
			DB rssDB = mongoClient.getDB("rssdbtest");
			rssColl = rssDB.getCollection("feeds");
			
		} catch (UnknownHostException e) {
			System.err.println("Problem with database host.");
			e.printStackTrace();
		} catch (MongoException e) {
			System.err.println("General Mongo problem.");
			e.printStackTrace();
		} catch (IllegalThreadStateException e) {
			System.err.println("Problem with threading.");
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	public static void closeDatabase() throws Exception {
		mongoClient.close();
	}
	
	@Parameters
	public static Collection<Object[]> data() throws Exception {
		setUpDatabase();
		Object[][] data = new Object[(int) rssColl.getCount()][2];
	
		// get all feeds
		DBCursor cursor = rssColl.find();
		try {
			int i = 0;
			while (cursor.hasNext()) {
				DBObject feed = cursor.next();
				data[i][0] = (String) feed.get("feedUrl");
				data[i][1] = new SyndFeedInput();
				i++;
			}
		} finally {
			cursor.close();
		}
		
		closeDatabase();
		return Arrays.asList(data);
	}

	/**
	 * Test method for {@link com.jernejerin.RSSThreadWorker#readFeed(java.lang.String, com.sun.syndication.io.SyndFeedInput)}.
	 * @throws Exception 
	 */
	@Test
	public void testReadFeed() throws Exception {
		try {
			assertNotNull("The value should not be null! URL: " + this.url, RSSThreadWorker.readFeed(this.url, this.input));
		} catch (Exception ex) {
			fail("Exception happened, which means problem with this URL: " + this.url);
		}
	}

}
