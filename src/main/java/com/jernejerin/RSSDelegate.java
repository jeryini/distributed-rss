/**
 * 
 */
package com.jernejerin;

import java.net.UnknownHostException;
import java.util.Date;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * This class represents Apache ActiveMQ
 * worker that delegates RSS feeds to queue.
 * 
 * @author Jernej Jerin
 *
 */
public class RSSDelegate {
	// default broker URL which means that JMS server is
	// on localhost
	public static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
	
	// name of the queue to whom we will be sending messages
	public static String subject = "RSSFEEDSQUEUE";
	
	// the time for checking for feeds that have not been used
	public static int seconds = 300;

	/**
	 * Implemented PointToPoint model because we 
	 * want only one of the consumers (RSSMainWorker)
	 * to receive the message.
	 * @param args
	 * @throws JMSException 
	 */
	public static void main(String[] args) throws JMSException {
		MongoClient mongoClient = null;
		Connection conn = null;
		try {
			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient( "localhost" , 27017 );
			DB rssDB = mongoClient.getDB("rssdb");
			DBCollection rssColl = rssDB.getCollection("feeds");
			
			// two queries, the first one returns feeds currently not used
			// and the second is for querying the feeds not used in last 5 minutes
			BasicDBObject queryFeedsUsed = new BasicDBObject("used", 0), queryLastAccessed = null;
			
			// connection to JMS server
			ConnectionFactory connFac = new ActiveMQConnectionFactory(url);
			conn = connFac.createConnection();
			conn.start();
			
			// create a non-transactional session for sending messages
			Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			// destination is our queue on JMS
			Destination dest = sess.createQueue(subject);
			
			// producer for sending messages
			MessageProducer msgProd = sess.createProducer(dest);
			
			// indefinetly check for feeds not in use
			while (true) {
				checkFeeds(queryFeedsUsed, queryLastAccessed, rssColl, msgProd, sess);
			}
			
		} catch (JMSException e) {
			System.err.println("Problem with JMS broker.");
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.err.println("Problem with database host.");
			e.printStackTrace();
		} catch (MongoException e) {
			System.err.println("General Mongo problem.");
			e.printStackTrace();
		} catch (IllegalThreadStateException e) {
			System.err.println("Problem with threading.");
		} finally {
			// close connections to mongodb and JMS
			conn.close();
			mongoClient.close();
		}
	}
	
	/**
	 * Check for feeds not used or not being accessed
	 * in last n seconds and then send message to the queue.
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
		MessageProducer msgProd, Session sess) throws JMSException {
	
		// get the first RSS feed that is currently not used
		DBObject feed = rssColl.findOne(queryFeedsUsed);
		
		if (feed != null)
			sendMessage(feed, rssColl, msgProd, sess);
		
		// the second query returns feeds that have not been used
		// in the past n seconds (n * 1000). This is a backup option 
		// in case that RSSThreadWorker shuts down unexpectedly and is not able to
		// write back the attribute used to 0.
		queryLastAccessed = new BasicDBObject("accessedAt", 
				new BasicDBObject("$lt", new Date(System.currentTimeMillis() - seconds * 1000)));
		feed = rssColl.findOne(queryLastAccessed);
		
		if (feed != null)
			sendMessage(feed, rssColl, msgProd, sess);
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
		String feedUrl = (String) feed.get("feedUrl");
		
		// update the used field and the last accessed date time field
		feed.put("accessedAt", new Date());
		feed.put("used", 1);
		rssColl.update(new BasicDBObject("feedUrl", feedUrl), feed);
		
		// send feed in the message to the queue
		TextMessage txtMsg = sess.createTextMessage(feed.toString());
		msgProd.send(txtMsg);
		System.out.println("Message sent '" + txtMsg.getText() + "'");
	}
}
