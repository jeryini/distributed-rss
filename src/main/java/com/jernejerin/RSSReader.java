/**
 * 
 */
package com.jernejerin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

/**
 * @author Jernej Jerin
 *
 */
public class RSSReader implements Runnable {
	private DBObject feedDB;
	private DBCollection rssColl;
	private DBCollection entriesColl;
	
	public RSSReader(DBObject feedDB, DBCollection rssColl, DBCollection entriesColl) {
		this.feedDB = feedDB;
		this.rssColl = rssColl;
		this.entriesColl = entriesColl;
	}

	public void run() {
		try {
			URL feedUrl = new URL((String) feedDB.get("uri"));
			
			// our input reader for rss
			SyndFeedInput input = new SyndFeedInput();
			
			while (true) {
				// read feed from the url
				SyndFeed feed = input.build(new XmlReader(feedUrl));
				
				// update last accessed time every second
				// and other attributes pertaining feed/channel
				feedDB.put("accessedAt", new Date());
				feedDB.put("title", feed.getTitle());
				feedDB.put("description", feed.getDescription());
				feedDB.put("link", feed.getLink());
				feedDB.put("pubDate", feed.getPublishedDate());
				
				rssColl.update(new BasicDBObject("uri", feedDB.get("uri")), feedDB);
				
				// get current local entries for this feed (NORMALIZED data models)
				// using One-To-Many Relationships
				// reason: http://blog.mongolab.com/2013/04/thinking-about-arrays-in-mongodb/
				
				
				// TODO: save new entries to mongo
				
				// check every second for new entries
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FeedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
