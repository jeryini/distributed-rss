package com.jernejerin;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

/**
 * This worker represent single virtual machine.
 * 
 * @author Jernej Jerin
 *
 */
public class RSSMainWorker {
	// queue size for active threads
	public static final int QUEUE_SIZE = 1000;
	
	// default broker URL which means that JMS server is
	// on localhost
	private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
	
	// name of the queue from whom we will be receiving messages
	private static String subject = "RSSFEEDSQUEUE";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		MongoClient mongoClient = null;
		Connection conn = null;
		try {
			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient( "localhost" , 27017 );
			DB rssDB = mongoClient.getDB("rssdb");
			DBCollection rssColl = rssDB.getCollection("feeds");
			DBCollection entriesColl = rssDB.getCollection("entries");
			
			// connection to JMS server
			ConnectionFactory connFac = new ActiveMQConnectionFactory(url);
			conn = connFac.createConnection();
			conn.start();
			
			// create a non-transactional session for sending messages
			Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			// destination is our queue on JMS
			Destination dest = sess.createQueue(subject);
			
			// consumer for receiving messages
			MessageConsumer msgCons = sess.createConsumer(dest);
			
			// create a thread pool with fixed number of threads
			// the same as using ThreadPoolExecutor with default values
			ThreadPoolExecutor executor = new ThreadPoolExecutor(QUEUE_SIZE, QUEUE_SIZE, 0L, 
					TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
			
			// check for available threads indefinetly
			while (true) {
				// create maximum of specified jobs
				if (executor.getActiveCount() < QUEUE_SIZE) {
					// get the available RSS feed from the message queue
					// this call is blocking!
					Message msg = msgCons.receive();
					
					if (msg instanceof TextMessage) {
						TextMessage txtMsg = (TextMessage) msg;
						
						// parse it from JSON to DBObject
						DBObject feed = (DBObject) JSON.parse(txtMsg.getText());
		
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
		} catch (JMSException e) {
			System.err.println("Problem with JMS.");
			e.printStackTrace();
		} finally {
			mongoClient.close();
		}
	}
}
