package com.jernejerin;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * This class represents main worker that delegates RSS feeds to message queue.
 * The delegation of the feeds to the queue is based on the fact if the feed is
 * currently in use or if some specified amount of time has passed since feed
 * was updated the last time.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
public class RSSDelegate {
	
	// default broker URL which means that JMS server is
	// on localhost
	public static final String URL = ActiveMQConnection.DEFAULT_BROKER_URL;

	// name of the queue to whom we will be sending messages
	public static final String SUBJECT = "RSSFEEDSQUEUE";

	// the time for checking for feeds that have not been used.
	// If it is not passed as argument then default time of 300s is used
	public static int seconds = 300;
	
	// logger for this class
	static Logger logger = Logger.getLogger(RSSDelegate.class);

	/**
	 * Implemented PointToPoint model because we want only one of the consumers
	 * (RSSMainWorker) to receive the message.
	 * 
	 * @param args
	 * @throws JMSException
	 */
	public static void main(String[] args) throws JMSException {
		Properties props = new Properties();
		MongoClient mongoClient = null;
		Connection conn = null;
		try {
			// configure logger
			props.load(new FileInputStream("log4j.properties"));
			PropertyConfigurator.configure(props);
			
			if (args.length > 0)
				seconds = Integer.parseInt(args[0]);

			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient("localhost", 27017);
			DB rssDB = mongoClient.getDB("rssdb");
			DBCollection rssColl = rssDB.getCollection("feeds");
			logger.info("Created connection to MongoDB.");

			// two queries, the first one returns feeds currently not used
			// and the second is for querying the feeds not used in last
			// seconds
			BasicDBObject queryFeedsUsed = new BasicDBObject("used", 0), queryLastAccessed = null;

			// connection to JMS server
			ConnectionFactory connFac = new ActiveMQConnectionFactory(URL);
			conn = connFac.createConnection();
			conn.start();
			logger.info("Created connection to ActiveMQ.");

			// create a non-transactional session for sending messages
			Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

			// destination is our queue on JMS
			Destination dest = sess.createQueue(SUBJECT);

			// producer for sending messages
			MessageProducer msgProd = sess.createProducer(dest);

			// indefinetly check for feeds
			while (true) {
				checkFeeds(queryFeedsUsed, queryLastAccessed, rssColl, msgProd,
						sess);
			}

		} catch (JMSException e) {
			logger.fatal("Problem with JMS: " + e.getMessage());
		} catch (UnknownHostException e) {
			logger.fatal("Problem with database host: " + e.getMessage());
		} catch (MongoException e) {
			logger.fatal("General Mongo problem: " + e.getMessage());
		} catch (IllegalThreadStateException e) {
			logger.fatal("Problem with threading: " + e.getMessage());
		} catch (FileNotFoundException e) {
			logger.fatal("Logger properties not found: " + e.getMessage());
		} catch (IOException e) {
			logger.fatal(e.getMessage());
		} finally {
			// close connections to MongoDB and ActiveMQ
			conn.close();
			mongoClient.close();
			logger.info("Closed connection to MongoDB and ActiveMQ");
		}
	}

	/**
	 * Check for feeds not used or not being updated in last n seconds and then
	 * send message containing feed to the queue.
	 * 
	 * @param queryFeedsUsed
	 * @param queryLastAccessed
	 * @param rssColl
	 * @param msgProd
	 * @param sess
	 * @throws JMSException
	 */
	public static void checkFeeds(BasicDBObject queryFeedsUsed,
			BasicDBObject queryLastAccessed, DBCollection rssColl,
			MessageProducer msgProd, Session sess) throws JMSException  {

		// get the first RSS feed that is currently not used
		DBObject feed = rssColl.findOne(queryFeedsUsed);

		if (feed != null) {
			logger.info("Feed " + feed.get("feedUrl") + " not used.");
			sendMessage(feed, rssColl, msgProd, sess);
		}

		// the second query returns feeds that have not been used
		// in the past n seconds (n * 1000). This is a backup option
		// in case that RSSThreadWorker shuts down unexpectedly and is not able
		// to write back the attribute used to 0.
		queryLastAccessed = new BasicDBObject("accessedAt", new BasicDBObject(
				"$lt", new Date(System.currentTimeMillis() - seconds * 1000)));
		feed = rssColl.findOne(queryLastAccessed);

		if (feed != null) {
			logger.info("Feed " + feed.get("feedUrl") + " has not been updated in last " + seconds + "s.");
			sendMessage(feed, rssColl, msgProd, sess);
		}
	}

	/**
	 * Update the feed attributes (accessedAt, used) and send it to the queue
	 * for the RSSMainWorker to pick it up.
	 * 
	 * @param feed
	 * @param rssColl
	 * @param msgProd
	 * @param sess
	 * @throws JMSException
	 */
	public static void sendMessage(DBObject feed, DBCollection rssColl,
			MessageProducer msgProd, Session sess) throws JMSException {
		// update the used field and the last accessed date time field
		feed.put("accessedAt", new Date());
		feed.put("used", 1);
		rssColl.update(new BasicDBObject("_id", feed.get("_id")), feed);

		// send feed in the message to the queue
		TextMessage txtMsg = sess.createTextMessage(feed.toString());
		msgProd.send(txtMsg);
		logger.info("Message sent '" + txtMsg.getText() + "'");
	}
	
}
