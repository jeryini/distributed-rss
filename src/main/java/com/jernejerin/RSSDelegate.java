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
 * worker that delegates RSS feeds to main
 * RSS workers.
 * 
 * @author Jernej Jerin
 *
 */
public class RSSDelegate {
	// default broker URL which means that JMS server is
	// on localhost
	private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
	
	// name of the queue to whom we will be sending messages
	private static String subject = "RSSFEEDSQUEUE";

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
			// 
			
			// we only need one instance of these classes for MongoDB
			// even with multiple threads -> thread safe
			mongoClient = new MongoClient( "localhost" , 27017 );
			DB rssDB = mongoClient.getDB("rssdb");
			DBCollection rssColl = rssDB.getCollection("feeds");
			
			// two queries, the first one returns feeds currently not used
			BasicDBObject queryFeedsUsed = new BasicDBObject("used", 0);
			
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
				// first query for feeds currently not used
				// get the available RSS feed from the message queue
				// get the first RSS feed that is currently not used
				DBObject feed = rssColl.findOne(queryFeedsUsed);
				
				if (feed != null)
					sendMessage(feed, rssColl, msgProd, sess);
				
				// the second query returns feeds that have not been used
				// in the past 5 minutes. This is a backup option.
				BasicDBObject queryLastAccessed = new BasicDBObject("accessedAt", 
						new BasicDBObject("$lt", new Date(System.currentTimeMillis() - 60 * 5 * 1000)));
				feed = rssColl.findOne(queryLastAccessed);
				
				if (feed != null)
					sendMessage(feed, rssColl, msgProd, sess);
				
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
	 * Update the feed as used and send it to the queue.
	 * 
	 * @param feed
	 * @param rssColl
	 * @param msgProd
	 * @param sess
	 * @throws JMSException
	 */
	private static void sendMessage(DBObject feed, DBCollection rssColl, 
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
