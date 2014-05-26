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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
	
	/** The default database's host address. */
	private static String hostDB = "localhost";

	/** The default port on which the database is running. */
	private static int portDB = 27017;

	/** The name of the database to use. */
	private static String dbName = "rssdb";

	/** The name of the collection to use. */
	private static String collName = "feeds";

	/** The URL of the broker. */
	public static String hostBroker = ActiveMQConnection.DEFAULT_BROKER_URL;

	/** Name of the queue to whom we will be sending messages */
	public static String subject = "RSSFEEDSQUEUE";

	/**
	 * The time for checking for feeds that have not been accessed in seconds.
	 * This depends on number of VM and threads per VM. If we have a lot of
	 * powerful VM, than this number should be lower. If it is not passed as
	 * argument then default time of 24h.
	 */
	public static int checkInterval = 24 * 60 * 60;

	/** Logger for this class. */
	private static final Logger LOG = Logger.getLogger(RSSDelegateWorker.class);

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
			
			// create Options object
			Options options = new Options();

			// add options
			options.addOption("help", false, "help for usage");
			options.addOption("hostDB", true, "database's host address");
			options.addOption("portDB", true,
					"port on which the database is running");
			options.addOption("dbName", true, "the name of the database to use");
			options.addOption("collName", true, "the name of collection to use");
			options.addOption("hostBroker", true,
					"the URL of the broker");
			options.addOption("subject", true,
					"name of the queue");
			options.addOption("checkInterval", true,
					"time in seconds for checking stalled feeds");

			// parser for command line arguments
			CommandLineParser parser = new GnuParser();
			CommandLine cmd = parser.parse(options, args);
			
			if (cmd.hasOption("help")) {
				HelpFormatter help = new HelpFormatter();
				help.printHelp("java -jar RSSDelegateWorker", options);
				System.exit(-1);
			}
			if (cmd.getOptionValue("hostDB") != null)
				hostDB = cmd.getOptionValue("hostDB");
			if (cmd.getOptionValue("portDB") != null)
				portDB = Integer.parseInt(cmd.getOptionValue("portDB"));
			if (cmd.getOptionValue("dbName") != null)
				dbName = cmd.getOptionValue("dbName");
			if (cmd.getOptionValue("collName") != null)
				collName = cmd.getOptionValue("collName");
			if (cmd.getOptionValue("hostBroker") != null)
				hostBroker = cmd.getOptionValue("hostBroker");
			if (cmd.getOptionValue("subject") != null)
				subject = cmd.getOptionValue("subject");
			if (cmd.getOptionValue("checkInterval") != null)
				checkInterval = Integer.parseInt(cmd.getOptionValue("checkInterval"));

			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient(hostDB, portDB);
			DB rssDB = mongoClient.getDB(dbName);
			DBCollection rssColl = rssDB.getCollection(collName);
			LOG.info("Created connection to MongoDB.");

			// query for querying the feeds not queued in last specified
			// seconds
			BasicDBObject queryLastAccessed = null;

			// connection to JMS server
			ConnectionFactory connFac = new ActiveMQConnectionFactory(hostBroker);
			conn = connFac.createConnection();
			conn.start();
			LOG.info("Created connection to ActiveMQ.");

			// create a non-transactional session for sending messages
			Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

			// destination is our queue on JMS
			Destination dest = sess.createQueue(subject);

			// producer for sending messages
			MessageProducer msgProd = sess.createProducer(dest);

			// put all the feeds into the queue
			DBCursor cursor = rssColl.find();
			System.out.println("Sending all feeds into queue.");
			try {
				while (cursor.hasNext())
					sendMessage(cursor.next(), rssColl, msgProd, sess);
			} finally {
				cursor.close();
			}

			// indefinetly check for stalled feeds (crashed VM-RSSMainWorker or
			// thread-RSSThreadWorker). So this is for solving unchecked
			// exceptions.
			System.out.println("Checking for stalled feeds...");
			while (true) {
				checkFeeds(queryLastAccessed, rssColl, msgProd, sess);
			}

		} catch (JMSException e) {
			LOG.fatal("Problem with JMS: " + e.getMessage());
		} catch (UnknownHostException e) {
			LOG.fatal("Problem with database host: " + e.getMessage());
		} catch (MongoException e) {
			LOG.fatal("General Mongo problem: " + e.getMessage());
		} catch (IllegalThreadStateException e) {
			LOG.fatal("Problem with threading: " + e.getMessage());
		} catch (FileNotFoundException e) {
			LOG.fatal("Logger properties not found: " + e.getMessage());
		} catch (IOException e) {
			LOG.fatal(e.getMessage());
		} catch (ParseException e) {
			LOG.fatal(e.getMessage());
		} finally {
			// close connections to MongoDB and ActiveMQ
			conn.close();
			mongoClient.close();
			LOG.info("Closed connection to MongoDB and ActiveMQ");
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
				"$lt", new Date(System.currentTimeMillis() - checkInterval * 1000)));
		DBObject feed = rssColl.findOne(queryLastAccessed);

		if (feed != null) {
			LOG.info("Feed " + feed.get("feedUrl")
					+ " has not been updated in last " + checkInterval + "s.");
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
		LOG.info("Message sent '" + txtMsg.getText() + "'");
	}

}
