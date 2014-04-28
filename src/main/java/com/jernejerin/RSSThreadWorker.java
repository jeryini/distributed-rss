package com.jernejerin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.sun.syndication.feed.synd.SyndCategory;
import com.sun.syndication.feed.synd.SyndEnclosure;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.feed.synd.SyndImage;
import com.sun.syndication.feed.synd.SyndPerson;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

/**
 * @author Jernej Jerin
 *
 */
public class RSSThreadWorker implements Runnable {
	private DBObject feedDB;
	private DBCollection rssColl;
	private DBCollection entriesColl;
	
	private final String USER_AGENT = "Mozilla/5.0";
	
	public RSSThreadWorker(DBObject feedDB, DBCollection rssColl, DBCollection entriesColl) {
		this.feedDB = feedDB;
		this.rssColl = rssColl;
		this.entriesColl = entriesColl;
	}

	//@SuppressWarnings({ "unchecked", "rawtypes" })
	/**
	 * Check for new entries every second for one feed infinite times.
	 */
	@SuppressWarnings("unchecked")
	public void run() {
		try {
			URL feedUrl = new URL((String) feedDB.get("feedUrl"));
			
			// our input reader for rss
			SyndFeedInput input = new SyndFeedInput();
			SyndFeed feed;
			
			while (true) {
				// read feed from the url
				feed = input.build(new XmlReader(feedUrl));
				
				// update the feed information
				feedUpdate(feedDB, feed);
				
				// get the list of entries that are saved for this feed
				ArrayList<Integer> idList = (ArrayList<Integer>) feedDB.get("entries");
				if (idList == null)
					idList = new ArrayList<Integer>();
				
				// get new entries
				ArrayList<DBObject> entriesDBNew = insertNewEntries(feed, idList);
				
				// TODO: Check for Apache Nutch (web crawler).
				
				// set reference to feed
				feedDB.put("entries", idList);
				
				// bulk inset new entries
				entriesColl.insert(entriesDBNew);
				
				// update feed which can contain new references to entries
				// and updated feed information
				rssColl.update(new BasicDBObject("feedUrl", feedDB.get("feedUrl")), feedDB);
				
				// check every 30 second for new entries
				Thread.sleep(30000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (FeedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// this feed is not used anymore
			feedDB.put("used", 0);
			rssColl.update(new BasicDBObject("feedUrl", feedDB.get("feedUrl")), feedDB);
		}
	}
	
	/**
	 * Insert new feed entries into DB.
	 * 
	 * @param feed
	 * @param idList
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private ArrayList<DBObject> insertNewEntries(SyndFeed feed,
			ArrayList<Integer> idList) {
		// current local entries for this feed use NORMALIZED data models
		// using One-To-Many Relationships
		// reason: http://blog.mongolab.com/2013/04/thinking-about-arrays-in-mongodb/
		ArrayList<DBObject> entriesDBNew = new ArrayList<DBObject>();
		for (SyndEntry entry : (ArrayList<SyndEntry>) feed.getEntries()) {
			// all elements of an item are optional, however at least 
			// one of title or description must be present -> we cannot 
			// trust user about this. That is why we will check first for
			// the guid, then url and then for description and the title
			// for inserting new entry check if that entry already exists
			// Because of possible hash collisions we Extend to 64-bit or use SHA-1.
			int id = 0;
			if (entry.getUri() != null)
				// first we check if uri (guid) is available
				id = entry.getUri().hashCode();
			else if(entry.getLink() != null)
				id = entry.getLink().hashCode();
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
			// lets instead check the array of references in the feed for this id
			// that way we save DB queries
			
			if (!idList.contains(id)) {
				// does not exist yet, save it to DB
				BasicDBObject entryDBNew = new BasicDBObject("_id", id);
				
				if (entry.getTitle() != null)
					entryDBNew.append("title", entry.getTitle());
				if (entry.getLink() != null) {
					entryDBNew.append("link", entry.getLink());
					
					try {
						// if link exists we can fetch the whole entry (HTML page)
						URL obj = new URL(entry.getLink());
						HttpURLConnection con = (HttpURLConnection) obj.openConnection();
				 
						// optional default is GET
						con.setRequestMethod("GET");
				 
						//add request header
						con.setRequestProperty("User-Agent", USER_AGENT);
				 
						if (con.getResponseCode() == 200) {
							BufferedReader in = new BufferedReader(
							        new InputStreamReader(con.getInputStream()));
							String inputLine;
							StringBuffer response = new StringBuffer();
					 
							while ((inputLine = in.readLine()) != null) {
								response.append(inputLine);
							}
							in.close();
					 
							// add to database
							entryDBNew.append("fullContent", response.toString());
						}
					} catch (MalformedURLException ex) {
						ex.printStackTrace();
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}
				if (entry.getDescription() != null)
					entryDBNew.append("description", entry.getDescription().getValue());
				if (entry.getAuthors() != null && entry.getAuthors().size() > 0) {
					ArrayList<BasicDBObject> authors = new ArrayList<BasicDBObject>();
					for (SyndPerson author : (ArrayList<SyndPerson>) entry.getAuthors()) {
						BasicDBObject authorDB = new BasicDBObject();
						
						if (author.getName() != null)
							authorDB.append("name", author.getName());
						if (author.getUri() != null)
							authorDB.append("uri", author.getUri());
						if (author.getName() != null || author.getUri() != null)
							authors.add(authorDB);
					}
					entryDBNew.put("authors", authors);
				}
				if (entry.getCategories() != null && entry.getCategories().size() > 0) {
					ArrayList<BasicDBObject> categories = new ArrayList<BasicDBObject>();
					for (SyndCategory category : (ArrayList<SyndCategory>) entry.getCategories()) {
						BasicDBObject categoryDB = new BasicDBObject();
						
						if (category.getName() != null)
							categoryDB.append("name", category.getName());
						if (category.getTaxonomyUri() != null)
							categoryDB.append("taxonomyURI", category.getTaxonomyUri());
						if (category.getName() != null || category.getTaxonomyUri() != null)
							categories.add(categoryDB);
					}
					entryDBNew.append("categories", categories);
				}
				// comments does not exist
				if (entry.getEnclosures() != null && entry.getEnclosures().size() > 0) {
					ArrayList<BasicDBObject> enclosures = new ArrayList<BasicDBObject>();
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
					entryDBNew.append("enclosure", enclosures);
				}
				if (entry.getUri() != null)
					entryDBNew.append("guid", entry.getUri());
				if (entry.getPublishedDate() != null)
					entryDBNew.append("pubDate", entry.getPublishedDate());
				// source?
				
				// insert new entry into list
				entriesDBNew.add(entryDBNew);
				
				// add id to list
				idList.add(id);
			}
		}
		
		return entriesDBNew;
	}

	/**
	 * Updates feed information such as accessed time and other attributes 
	 * pertaining feed/channel.
	 * 
	 * @param feedDB
	 * @param feed
	 */
	@SuppressWarnings("unchecked")
	private void feedUpdate(DBObject feedDB, SyndFeed feed) {
		// this field is custom
		feedDB.put("accessedAt", new Date());
		
		// REQUIRED channel elements as defined in RSS 2.0 Specification
		// http://cyber.law.harvard.edu/rss/rss.html#
		// even though this elements are required by specification we 
		// cannot trust users
		if (feed.getTitle() != null)
			feedDB.put("title", feed.getTitle());
		if (feed.getLink() != null)
			feedDB.put("link", feed.getLink());
		if (feed.getDescription() != null)
			feedDB.put("description", feed.getDescription());
		
		// OPTIONAL channel elements as defined in RSS 2.0 Specification
		if (feed.getLanguage() != null)
			feedDB.put("language", feed.getLanguage());
		if (feed.getCopyright() != null)
			feedDB.put("copyright", feed.getCopyright());
		// no specific get method for managing editor and web master. Using
		// authors instead
		if (feed.getAuthors() != null && feed.getAuthors().size() > 0) {
			ArrayList<BasicDBObject> authors = new ArrayList<BasicDBObject>();
			for (SyndPerson author : (ArrayList<SyndPerson>) feed.getAuthors()) {
				BasicDBObject authorDB = new BasicDBObject();
				
				if (author.getName() != null)
					authorDB.append("name", author.getName());
				if (author.getUri() != null)
					authorDB.append("uri", author.getUri());
				if (author.getName() != null || author.getUri() != null)
					authors.add(authorDB);
			}
			feedDB.put("authors", authors);
		}
		if (feed.getPublishedDate() != null)
			feedDB.put("pubDate", feed.getPublishedDate());
		// last build date does not exist
		if (feed.getCategories() != null && feed.getCategories().size() > 0) {
			ArrayList<BasicDBObject> categories = new ArrayList<BasicDBObject>();
			for (SyndCategory category : (ArrayList<SyndCategory>) feed.getCategories()) {
				BasicDBObject categoryDB = new BasicDBObject();
				
				if (category.getName() != null)
					categoryDB.append("name", category.getName());
				if (category.getTaxonomyUri() != null)
					categoryDB.append("taxonomyURI", category.getTaxonomyUri());
				if (category.getName() != null || category.getTaxonomyUri() != null)
					categories.add(categoryDB);
			}
			feedDB.put("category", categories);
		}
		// generator does not exist
		// docs does not exist
		// cloud does not exist
		// ttl does not exist
		SyndImage feedImage = feed.getImage();
		if (feedImage != null) {
			BasicDBObject imageDB = new BasicDBObject();
			if (feedImage.getUrl() != null)
				imageDB.append("url", feedImage.getUrl());
			if (feedImage.getTitle() != null)
				imageDB.append("title", feedImage.getTitle());
			if (feedImage.getLink() != null)
				imageDB.append("link", feedImage.getLink());
			if (feedImage.getDescription() != null)
				imageDB.append("description", feedImage.getDescription());
			if (feedImage.getUrl() != null || feedImage.getTitle() != null || 
					feedImage.getLink() != null || feedImage.getDescription() != null)
				feedDB.put("image", imageDB);
			// width does not exist
			// height does not exist
		}
		// rating does not exist
		// text input does not exist
		// skip hours does not exist
		// skip days does not exist
	}
}