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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
	
	/** The default database's host address. */
	private static String hostDB = "localhost";

	/** The default port on which the database is running. */
	private static int portDB = 27017;

	/** The name of the database to use. */
	private static String dbName = "rssdb";

	/** The name of the collection to use. */
	private static String collNameFeeds = "feeds";
	
	/** The name of the collection to use. */
	private static String collNameEntries = "entries";

	/** The URL of the broker. */
	private static String hostBroker = ActiveMQConnection.DEFAULT_BROKER_URL;

	/** Name of the queue to whom we will be sending messages */
	private static String subject = "RSSFEEDSQUEUE";
	
	/** Queue size or number of active threads. Default is 10. */
	private static int threadsNum = 10;

	// LOG for this class
	private static final Logger LOG = Logger.getLogger(RSSMainWorker.class);

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
			// configure LOG
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
			options.addOption("collNameFeeds", true, "the name of collection to use for feeds");
			options.addOption("collNameEntries", true, "the name of collection to use for entries");
			options.addOption("hostBroker", true,
					"the URL of the broker");
			options.addOption("subject", true,
					"name of the queue");
			options.addOption("threadsNum", true, "number of active threads");

			// parser for command line arguments
			CommandLineParser parser = new GnuParser();
			CommandLine cmd = parser.parse(options, args);
			
			if (cmd.hasOption("help")) {
				HelpFormatter help = new HelpFormatter();
				help.printHelp("java -jar RSSMainWorker", options);
				System.exit(-1);
			}
			if (cmd.getOptionValue("hostDB") != null)
				hostDB = cmd.getOptionValue("hostDB");
			if (cmd.getOptionValue("portDB") != null)
				portDB = Integer.parseInt(cmd.getOptionValue("portDB"));
			if (cmd.getOptionValue("dbName") != null)
				dbName = cmd.getOptionValue("dbName");
			if (cmd.getOptionValue("collNameFeeds") != null)
				collNameFeeds = cmd.getOptionValue("collNameFeeds");
			if (cmd.getOptionValue("collNameEntries") != null)
				collNameEntries = cmd.getOptionValue("collNameEntries");
			if (cmd.getOptionValue("hostBroker") != null)
				hostBroker = cmd.getOptionValue("hostBroker");
			if (cmd.getOptionValue("subject") != null)
				subject = cmd.getOptionValue("subject");
			if (cmd.getOptionValue("threadsNum") != null)
				threadsNum = Integer.parseInt(cmd.getOptionValue("threadsNum"));

			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient(hostDB, portDB);
			DB rssDB = mongoClient.getDB(dbName);
			DBCollection rssColl = rssDB.getCollection(collNameFeeds);
			DBCollection entriesColl = rssDB.getCollection(collNameEntries);
			LOG.info("Created connection to MongoDB.");

			// connection to JMS server. ConnectionFactory and Connection are
			// thread safe!
			ActiveMQConnectionFactory connFac = new ActiveMQConnectionFactory(
					hostBroker);
			conn = connFac.createConnection();
			conn.start();
			LOG.info("Created connection to ActiveMQ.");

			// create a non-transactional session for sending messages with
			// client acknowledge. We need none JMS compatible acknowledge, i.e.
			// vendor specific
			ActiveMQSession sess = (ActiveMQSession) conn.createSession(false,
					ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);

			// destination is our queue on JMS
			Destination dest = sess.createQueue(subject);

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
					LOG.info("New thread available.");
					
					// get the available RSS feed from the message queue
					// this call is blocking!
					Message msg = (Message) msgCons.receive();
					LOG.info("Received new job from queue.");

					if (msg instanceof TextMessage) {
						TextMessage txtMsg = (TextMessage) msg;

						// parse it from JSON to DBObject
						DBObject feedDB = (DBObject) JSON.parse(txtMsg
								.getText());

						// start thread for given RSS feed
						Runnable rssThreadWorker = new RSSThreadWorker(msg, feedDB,
								rssColl, entriesColl, conn, subject);
						executor.execute(rssThreadWorker);
						LOG.info("New thread started for feed "
								+ feedDB.get("feedUrl"));
					}
				}
			}
		} catch (UnknownHostException e) {
			LOG.fatal("Problem with database host: " + e.getMessage());
		} catch (MongoException e) {
			LOG.fatal("General Mongo problem: " + e.getMessage());
		} catch (IllegalThreadStateException e) {
			LOG.fatal("Problem with threading: " + e.getMessage());
		} catch (JMSException e) {
			LOG.fatal("Problem with JMS: " + e.getMessage());
		} catch (FileNotFoundException e) {
			LOG.fatal(e.getMessage());
		} catch (IOException e) {
			LOG.fatal(e.getMessage());
		} catch (ParseException e) {
			LOG.fatal(e.getMessage());
		} finally {
			conn.close();
			mongoClient.close();
			LOG.info("Closed connection to MongoDB and ActiveMQ");
		}
	}
}
