package com.jernejerin;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.Message;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

/**
 * This class represents main worker for a single virtual machine. It dequeues
 * messages from queue and creates thread for each RSS feed. This thread then
 * process the feed checking for new entries and other data pertaining the feed.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
public class RSSMainWorker {
	// queue size or number of active threads. Can be passed as argument.
	public static int threadsNum = 50;

	// default broker URL which means that JMS server is
	// on localhost
	private static final String URL = ActiveMQConnection.DEFAULT_BROKER_URL;

	// name of the queue from whom we will be receiving messages
	private static final String SUBJECT = "RSSFEEDSQUEUE";

	// logger for this class
	static Logger logger = Logger.getLogger(RSSMainWorker.class);

	/**
	 * Checks for available threads and messages from queue in infinite loop.
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
				threadsNum = Integer.parseInt(args[0]);

			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient("localhost", 27017);
			DB rssDB = mongoClient.getDB("rssdb");
			DBCollection rssColl = rssDB.getCollection("feeds");
			DBCollection entriesColl = rssDB.getCollection("entries");
			logger.info("Created connection to MongoDB.");

			// connection to JMS server. ConnectionFactory and Connection are
			// thread safe!
			ActiveMQConnectionFactory connFac = new ActiveMQConnectionFactory(
					URL);
			conn = connFac.createConnection();
			conn.start();
			logger.info("Created connection to ActiveMQ.");

			// create a non-transactional session for sending messages with
			// client acknowledge. We need none JMS compatible acknowledge, i.e.
			// vendor specific
			ActiveMQSession sess = (ActiveMQSession) conn.createSession(false,
					ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);

			// destination is our queue on JMS
			Destination dest = sess.createQueue(SUBJECT);

			// consumer for receiving messages
			MessageConsumer msgCons = sess.createConsumer(dest);

			// create a thread pool with fixed number of threads
			// the same as using ThreadPoolExecutor with default values
			ThreadPoolExecutor executor = new ThreadPoolExecutor(threadsNum,
					threadsNum, 0L, TimeUnit.SECONDS,
					new LinkedBlockingQueue<Runnable>());

			// check for available threads indefinetly
			while (true) {
				// create maximum of specified threads
				if (executor.getActiveCount() < threadsNum) {
					logger.info("New thread available.");
					// get the available RSS feed from the message queue
					// this call is blocking!
					Message msg = (Message) msgCons.receive();
					logger.info("Received new job from queue.");

					if (msg instanceof TextMessage) {
						TextMessage txtMsg = (TextMessage) msg;

						// parse it from JSON to DBObject
						DBObject feedDB = (DBObject) JSON.parse(txtMsg
								.getText());

						// start thread for given RSS feed
						Runnable rssThreadWorker = new RSSThreadWorker(msg, feedDB,
								rssColl, entriesColl, conn);
						executor.execute(rssThreadWorker);
						logger.info("New thread started for feed "
								+ feedDB.get("feedUrl"));
					}
				}
			}
		} catch (UnknownHostException e) {
			logger.fatal("Problem with database host: " + e.getMessage());
		} catch (MongoException e) {
			logger.fatal("General Mongo problem: " + e.getMessage());
		} catch (IllegalThreadStateException e) {
			logger.fatal("Problem with threading: " + e.getMessage());
		} catch (JMSException e) {
			logger.fatal("Problem with JMS: " + e.getMessage());
		} catch (FileNotFoundException e) {
			logger.fatal(e.getMessage());
		} catch (IOException e) {
			logger.fatal(e.getMessage());
		} finally {
			conn.close();
			mongoClient.close();
			logger.info("Closed connection to MongoDB and ActiveMQ");
		}
	}
}
