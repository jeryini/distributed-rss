/**
 * 
 */
package com.jernejerin.test;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jernejerin.RSSDelegateWorker;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

/**
 * Test for class RSSDelegate.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
public class RSSDelegateTest {
	// logger for this class
	static Logger logger = Logger.getLogger(RSSDelegateTest.class);

	public static String subject = "RSSFEEDSQUEUETEST";
	static MongoClient mongoClient = null;
	static Connection conn = null;
	static DBCollection rssColl;
	static BasicDBObject queryFeedsUsed, queryLastAccessed = null;
	static Session sess;
	static MessageProducer msgProd;
	static MessageConsumer msgCons;
	static Destination dest = null;

	/**
	 * Set up MongoDB and ActiveMQ connection. Also add test examples to
	 * mongodb.
	 * 
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Properties props = new Properties();
		try {
			// configure logger
			props.load(new FileInputStream("log4j.properties"));
			PropertyConfigurator.configure(props);
			
			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient("localhost", 27017);

			// using test database
			DB rssDB = mongoClient.getDB("rssdbtest");
			rssColl = rssDB.getCollection("feeds");

			// two queries, the first one returns feeds currently not used
			// and the second is for querying the feeds not used in last 5
			// minutes
			queryFeedsUsed = new BasicDBObject("used", 0);

			// connection to JMS server
			ConnectionFactory connFac = new ActiveMQConnectionFactory(
					RSSDelegateWorker.URL);
			conn = connFac.createConnection();
			conn.start();

			// create a non-transactional session for sending messages
			sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

			// destination is our queue on JMS
			dest = sess.createQueue(subject);

			// producer for sending messages
			msgProd = sess.createProducer(dest);

			// consumer for receiving messages
			msgCons = sess.createConsumer(dest);
		} catch (JMSException e) {
			logger.fatal(e.getMessage());
		} catch (UnknownHostException e) {
			logger.fatal(e.getMessage());
		} catch (MongoException e) {
			logger.fatal(e.getMessage());
		} catch (IllegalThreadStateException e) {
			logger.fatal(e.getMessage());
		}
	}

	/**
	 * Clear DB and close connections to MongoDB and ActiveMQ.
	 * 
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		// clear DB (pass an empty BasicDBObject and the entire
		// collection will be deleted
		rssColl.remove(new BasicDBObject());

		conn.close();
		mongoClient.close();
	}

	/**
	 * Test method for
	 * {@link com.jernejerin.RSSDelegateWorker#checkFeeds(com.mongodb.BasicDBObject, com.mongodb.BasicDBObject, com.mongodb.DBCollection, javax.jms.MessageProducer, javax.jms.Session)}
	 * .
	 * 
	 * @throws JMSException
	 */
	@Test
	public void testCheckFeeds() throws JMSException {
		// purge DB
		rssColl.remove(new BasicDBObject());

		// add test data
		BasicDBObject rssFeed = new BasicDBObject("feedUrl",
				"http://0.tqn.com/6/g/sbinfocanada/b/rss2.xml").append("used",
				0);
		rssColl.insert(rssFeed);

		// for testing that enough time has passed to trigger second query
		rssFeed = new BasicDBObject("feedUrl",
				"http://100meterijs.wordpress.com/feed/").append("used", 1)
				.append("accessedAt",
						new Date(System.currentTimeMillis() - 500 * 1000));
		rssColl.insert(rssFeed);

		// for testing that not enough time has passed
		rssFeed = new BasicDBObject("feedUrl",
				"http://100meterijs.wordpress.com/feed/").append("used", 1)
				.append("accessedAt",
						new Date(System.currentTimeMillis() - 200 * 1000));
		rssColl.insert(rssFeed);

		for (int i = 0; i < 3; i++) {
			RSSDelegateWorker.checkFeeds(queryFeedsUsed, queryLastAccessed, rssColl,
					msgProd, sess);
		}

		int msgCount = 0;
		// retrieve messages
		while (msgCons.receiveNoWait() != null)
			msgCount++;

		// there should be only 2 messages in queue
		assertEquals("Only two messages should be in queue!", 2, msgCount);
	}

	/**
	 * 
	 * 
	 * Test method for
	 * {@link com.jernejerin.RSSDelegateWorker#sendMessage(com.mongodb.DBObject, com.mongodb.DBCollection, javax.jms.MessageProducer, javax.jms.Session)}
	 * .
	 * 
	 * @throws JMSException
	 */
	@Test
	public void testSendMessage() throws JMSException {
		// purge DB
		rssColl.remove(new BasicDBObject());

		// add test data to DB
		BasicDBObject rssFeed = new BasicDBObject("feedUrl",
				"http://0.tqn.com/6/g/sbinfocanada/b/rss2.xml").append("used",
				0);
		rssColl.insert(rssFeed);

		DBObject feed = rssColl.findOne(queryFeedsUsed);

		if (feed != null) {
			RSSDelegateWorker.sendMessage(feed, rssColl, msgProd, sess);

			// get the values of attributes that presumably method
			// send message should set
			int used = Integer.parseInt(feed.get("used").toString());
			DateTime accessedAt = new DateTime(feed.get("accessedAt"));

			// get the changed feed (i.e. update version in DB)
			BasicDBObject query = new BasicDBObject("_id", feed.get("_id"));
			DBObject feedDB = rssColl.findOne(query);
			int usedDB = Integer.parseInt(feedDB.get("used").toString());
			DateTime accessedAtDB = new DateTime(feedDB.get("accessedAt"));

			// test for equality of values used and accessed date time
			assertEquals("Used must be set to 1", used, usedDB);
			assertEquals("Date time value does not match",
					accessedAt.getMillis(), accessedAtDB.getMillis());

			// check that content of message in queue is the same
			Message msg = msgCons.receive();

			if (msg instanceof TextMessage) {
				TextMessage txtMsg = (TextMessage) msg;

				// parse it from JSON to DBObject
				DBObject feedQE = (DBObject) JSON.parse(txtMsg.getText());
				int usedQE = Integer.parseInt(feedQE.get("used").toString());
				DateTime accessedAtQE = new DateTime(feedQE.get("accessedAt"));

				// test for equality of values used and accessed date time
				assertEquals("Used must be set to 1", used, usedQE);
				assertEquals("Date time value does not match",
						accessedAt.getMillis(), accessedAtQE.getMillis());
			} else {
				fail("Message must be of type Text!");
			}
		} else {
			fail("There should be atleast one message in DB!");
		}
	}

}
