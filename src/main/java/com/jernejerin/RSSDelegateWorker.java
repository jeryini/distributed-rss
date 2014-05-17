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
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * This class represents delegate worker that delegates RSS feeds to message
 * queue. The delegation of the feeds to the queue is based on the fact if some
 * specified amount of time has passed since feed was updated the last time. The
 * worker first pushes all the feeds into the queue and then indefinitely checks
 * for last updated time.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
public class RSSDelegateWorker {

	/** Default broker URL. */
	public static final String URL = ActiveMQConnection.DEFAULT_BROKER_URL;

	/** Name of the queue to whom we will be sending messages */
	public static final String SUBJECT = "RSSFEEDSQUEUE";

	/**
	 * The time for checking for feeds that have not been accessed in seconds.
	 * This depends on number of VM and threads per VM. If we have a lot of
	 * powerful VM, than this number should be lower. If it is not passed as
	 * argument then default time of 120min.
	 */
	public static int seconds = 120 * 60;

	/** Logger for this class. */
	static Logger logger = Logger.getLogger(RSSDelegateWorker.class);

	/**
	 * Implemented PointToPoint (Queue) model because we want only one of the
	 * consumers (RSSMainWorker) to receive the message.
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

			// query for querying the feeds not queued in last specified
			// seconds
			BasicDBObject queryLastAccessed = null;

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

			// put all the feeds into the queue
			DBCursor cursor = rssColl.find();
			try {
				while (cursor.hasNext())
					sendMessage(cursor.next(), rssColl, msgProd, sess);
			} finally {
				cursor.close();
			}

			// indefinetly check for stalled feeds (crashed VM-RSSMainWorker or
			// thread-RSSThreadWorker). So this is for solving unchecked
			// exceptions.
			while (true) {
				checkFeeds(queryLastAccessed, rssColl, msgProd, sess);
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
	 * Check for feeds not being updated in last n seconds and then send message
	 * containing feed to the queue.
	 * 
	 * @param queryLastAccessed
	 * @param rssColl
	 * @param msgProd
	 * @param sess
	 * @throws JMSException
	 */
	public static void checkFeeds(BasicDBObject queryLastAccessed,
			DBCollection rssColl, MessageProducer msgProd, Session sess)
			throws JMSException {
		/*
		 * The second query returns feeds that have not been queued in the past
		 * n seconds (n * 1000). This is a backup option for in case that
		 * RSSThreadWorker shuts down unexpectedly and is not able to put the
		 * feed into queue.
		 */
		queryLastAccessed = new BasicDBObject("accessedAt", new BasicDBObject(
				"$lt", new Date(System.currentTimeMillis() - seconds * 1000)));
		DBObject feed = rssColl.findOne(queryLastAccessed);

		if (feed != null) {
			logger.info("Feed " + feed.get("feedUrl")
					+ " has not been updated in last " + seconds + "s.");
			sendMessage(feed, rssColl, msgProd, sess);
		}
	}

	/**
	 * Update the feed attributes (accessedAt) and send it to the queue
	 * for the RSSMainWorker to dequeued it.
	 * 
	 * @param feed
	 * @param rssColl
	 * @param msgProd
	 * @param sess
	 * @throws JMSException
	 */
	public static void sendMessage(DBObject feed, DBCollection rssColl,
			MessageProducer msgProd, Session sess) throws JMSException {
		// update the last accessed date time field
		feed.put("accessedAt", new Date());
		rssColl.update(new BasicDBObject("_id", feed.get("_id")), feed);

		// send feed in the message to the queue
		TextMessage txtMsg = sess.createTextMessage(feed.toString());
		msgProd.send(txtMsg);
		logger.info("Message sent '" + txtMsg.getText() + "'");
	}

}
