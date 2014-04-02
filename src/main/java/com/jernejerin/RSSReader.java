/**
 * 
 */
package com.jernejerin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.sun.syndication.feed.synd.SyndEnclosure;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.feed.synd.SyndImage;
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	/**
	 * Check for new entries every second for one feed infinite times.
	 */
	public void run() {
		try {
			URL feedUrl = new URL((String) feedDB.get("feedUrl"));
			
			// our input reader for rss
			SyndFeedInput input = new SyndFeedInput();
			BasicDBObject query;
			DBObject entryLocal;
			SyndFeed feed;
			
			while (true) {
				// read feed from the url
				feed = input.build(new XmlReader(feedUrl));
				
				// update last accessed time and other attributes 
				// pertaining feed/channel
				// this field is custom
				feedDB.put("accessedAt", new Date());
				
				// REQUIRED channel elements as defined in RSS 2.0 Specification
				// http://cyber.law.harvard.edu/rss/rss.html
				feedDB.put("title", feed.getTitle());
				feedDB.put("link", feed.getLink());
				feedDB.put("description", feed.getDescription());
				
				// OPTIONAL channel elements as defined in RSS 2.0 Specification
				if (feed.getLanguage() != null)
					feedDB.put("language", feed.getLanguage());
				if (feed.getCopyright() != null)
					feedDB.put("copyright", feed.getCopyright());
				// no specific get method for managing editor and web master. Using
				// authors instead
				if (feed.getAuthors() != null && feed.getAuthors().size() > 0)
					feedDB.put("authors", feed.getAuthors());
				if (feed.getPublishedDate() != null)
					feedDB.put("pubDate", feed.getPublishedDate());
				// last build date does not exist
				if (feed.getCategories() != null && feed.getCategories().size() > 0)
					feedDB.put("category", feed.getCategories());
				// generator does not exist
				// docs does not exist
				// cloud does not exist
				// ttl does not exist
				SyndImage feedImage = feed.getImage();
				if (feedImage != null) {
					BasicDBObject imageDB = new BasicDBObject("url", feedImage.getUrl()).
														append("title", feedImage.getTitle()).
														append("link", feedImage.getLink());
					if (feedImage.getDescription() != null)
						imageDB.append("description", feedImage.getDescription());
					feedDB.put("image", imageDB);
					// width does not exist
					// height does not exist
				}
				// rating does not exist
				// text input does not exist
				// skip hours does not exist
				// skip days does not exist
				
				
				// current local entries for this feed use NORMALIZED data models
				// using One-To-Many Relationships
				// reason: http://blog.mongolab.com/2013/04/thinking-about-arrays-in-mongodb/
				for (SyndEntry entry : (ArrayList<SyndEntry>) feed.getEntries()) {
					// all elements of an item are optional, however at least 
					// one of title or description must be present
					// for inserting new entry check if that entry already exists
					int id = 0;
					if (entry.getUri() != null)
						// first we check if uri (guid) is available
						id = entry.getUri().hashCode();
					else if (entry.getDescription() != null && entry.getTitle() != null)
						// then we check if description and title are available
						id = (entry.getDescription() + entry.getTitle()).hashCode();
					else if (entry.getDescription() != null)
						// only description
						id = entry.getDescription().hashCode();
					else if (entry.getTitle() != null)
						// only title
						id = entry.getTitle().hashCode();
					// This is bad solution to query DB for each entry if it already exists
					// query = new BasicDBObject("_id", id);
					// entryLocal = entriesColl.findOne(query);
					// Lets check the array of references in the feed for this id
					
					
					// TODO: save reference into array of feedDB
					// TODO: update after for each statement -> BULK UPDATE!!!
					// TODO: what about changes in entries? HASH FUNCTIONS!!!
					// TODO: two arrays, one for references containing uri, the other hash values of whole entry
					
					
					if (!((ArrayList<Integer>)feedDB.get("entries")).contains(id)) {
						// does not exist yet, save it to DB
						BasicDBObject entryNew = new BasicDBObject("_id", id);
						
						if (entry.getTitle() != null)
							entryNew.append("title", entry.getTitle());
						if (entry.getLink() != null)
							entryNew.append("link", entry.getLink());
						if (entry.getDescription() != null)
							entryNew.append("description", entry.getDescription().getValue());
						if (entry.getAuthors() != null && entry.getAuthors().size() > 0)
							entryNew.append("authors", entry.getAuthors());
						if (entry.getCategories() != null && entry.getCategories().size() > 0)
							entryNew.append("categories", entry.getCategories());
						// comments does not exist
						if (entry.getEnclosures() != null && entry.getEnclosures().size() > 0) {
							ArrayList enclosures = new ArrayList();
							for (SyndEnclosure enclosure : (ArrayList<SyndEnclosure>) entry.getEnclosures()) {
								BasicDBObject enclosureDB = new BasicDBObject();
								
								// all three attributes are required but we cannot trust the user
								if (enclosure.getUrl() != null)
									enclosureDB.append("url", enclosure.getUrl());
								if (enclosure.getLength() != 0)
									enclosureDB.append("length", enclosure.getLength());
								if (enclosure.getType() != null)
									enclosureDB.append("type", enclosure.getType());
								enclosures.add(enclosureDB);
							}
							entryNew.append("enclosure", enclosures);
						}
						if (entry.getUri() != null)
							entryNew.append("guid", entry.getUri());
						if (entry.getPublishedDate() != null)
							entryNew.append("pubDate", entry.getPublishedDate());
						
						// insert new entry into DB
						entriesColl.insert(entryNew);
					}
				}
				
				// update feed which can contain new references to entries
				// and updated feed information
				rssColl.update(new BasicDBObject("feedUrl", feedDB.get("feedUrl")), feedDB);
				
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
