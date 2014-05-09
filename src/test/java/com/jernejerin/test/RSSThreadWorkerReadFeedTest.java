package com.jernejerin.test;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
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
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;

/**
 * Parametrized test for read feed function in class RSSThreadWorker.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
@RunWith(Parameterized.class)
public class RSSThreadWorkerReadFeedTest {
	// logger for this class
	static Logger logger = Logger.getLogger(RSSDelegateTest.class);

	static MongoClient mongoClient = null;
	static DBCollection rssColl;

	private String url;
	private SyndFeedInput input;

	public RSSThreadWorkerReadFeedTest(String url, SyndFeedInput input) {
		this.url = url;
		this.input = input;
	}

	/**
	 * Sets up connection to MongoDB.
	 * 
	 * @throws java.lang.Exception
	 */
	public static void setUpDatabase() throws Exception {
		try {
			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient("localhost", 27017);

			// using test database
			DB rssDB = mongoClient.getDB("rssdbtest");
			rssColl = rssDB.getCollection("feeds");

			logger.info("Connected to DB.");
		} catch (UnknownHostException e) {
			logger.fatal(e.getMessage());
		} catch (MongoException e) {
			logger.fatal(e.getMessage());
		} catch (IllegalThreadStateException e) {
			logger.fatal(e.getMessage());
		}
	}

	/**
	 * Closes connection to MongoDB.
	 * 
	 * @throws java.lang.Exception
	 */
	public static void closeDatabase() throws Exception {
		mongoClient.close();
		logger.info("Closed DB.");
	}

	/**
	 * Get the data for multiple test. Each test has values for specific feed.
	 * 
	 * @return
	 * @throws Exception
	 */
	@Parameters
	public static Collection<Object[]> data() throws Exception {
		Properties props = new Properties();
		// configure logger
		props.load(new FileInputStream("log4j.properties"));
		PropertyConfigurator.configure(props);

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
	 * Test method for read feed. Checks if we get back the feed object.
	 * {@link com.jernejerin.RSSThreadWorker#readFeed(java.lang.String, com.sun.syndication.io.SyndFeedInput)}
	 * .
	 * 
	 * @throws Exception
	 */
	@Test
	public void testReadFeed() throws Exception {
		try {
			SyndFeed feed = RSSThreadWorker.readFeed(this.url, this.input);
			if (feed == null)
				logger.info("The value should not be null! URL: " + this.url);
			assertNotNull("The value should not be null! URL: " + this.url,
					feed);
		} catch (Exception ex) {
			logger.error("Exception happened, which means problem with this URL: "
					+ this.url);
			fail("Exception happened, which means problem with this URL: "
					+ this.url);
		}
	}

}
