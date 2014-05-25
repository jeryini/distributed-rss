package com.jernejerin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.Message;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

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
import com.sun.syndication.io.ParsingFeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

/**
 * This class represents thread worker. The thread is created in class
 * RSSMainWorker. This thread process the feed, i.e. updating feed data, getting
 * new entries, etc. The processing of xml is done with Rome library. The HTTP
 * GET is done with Apache HttpClient library.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
public class RSSThreadWorker implements Runnable {
	private static final String USER_AGENT = "Mozilla/5.0 Firefox/26.0";

	// logger for this class
	private static final Logger LOG = Logger.getLogger(RSSThreadWorker.class);

	private Message msg;
	private DBObject feedDB;
	private DBCollection rssColl;
	private DBCollection entriesColl;
	private Connection conn;
	private String subject;

	public RSSThreadWorker(Message msg, DBObject feedDB, DBCollection rssColl,
			DBCollection entriesColl, Connection conn, String subject) {
		this.msg = msg;
		this.feedDB = feedDB;
		this.rssColl = rssColl;
		this.entriesColl = entriesColl;
		this.conn = conn;
		this.subject = subject;
	}

	/**
	 * Check for new entries every second for one feed infinite times.
	 */
	@SuppressWarnings("unchecked")
	public void run() {
		Properties props = new Properties();
		try {
			// configure LOG
			props.load(new FileInputStream("log4j.properties"));
			PropertyConfigurator.configure(props);

			// input reader for rss
			SyndFeedInput input = new SyndFeedInput();
			SyndFeed feed = readFeed((String) feedDB.get("feedUrl"), input);

			if (feed != null) {
				LOG.info("Successfully read feed " + feedDB.get("feedUrl"));
				// update the feed information
				feedUpdate(feedDB, feed);

				/*
				 * Get the list of entries that are already saved for this feed.
				 * With this information we will be able to distinct between new
				 * entries of the given feed. Each feed contains in array SHA-1
				 * hash id of entries that belong to that feed. This way we
				 * don't have to do additionaly query of existing entries. The
				 * id is a SHA-1 digest of the uri, link, description or title.
				 */
				ArrayList<String> idList = (ArrayList<String>) feedDB
						.get("entries");
				if (idList == null)
					idList = new ArrayList<String>();

				// get new entries and save them
				getNewEntries(feed, idList);
			} else
				LOG.info("Problem with reading feed " + feedDB.get("feedUrl"));
		} catch (FileNotFoundException e) {
			LOG.fatal(e.getMessage());
		} catch (SecurityException e) {
			LOG.fatal(e.getMessage());
		} catch (IllegalArgumentException e) {
			LOG.fatal(e.getMessage());
		} catch (InterruptedException e) {
			LOG.fatal(e.getMessage());
		} catch (MalformedURLException e) {
			LOG.fatal(e.getMessage());
		} catch (ParsingFeedException e) {
			LOG.fatal(e.getMessage());
		} catch (FeedException e) {
			LOG.fatal(e.getMessage());
		} catch (IOException e) {
			LOG.fatal(e.getMessage());
		} catch (Exception ex) {
			LOG.fatal(ex.getMessage());
		} finally {
			/*
			 * Put it back to the queue because Session and MessageProducer are
			 * not Thread safe we need to create each of them in sepearate
			 * thread ConnectionFactory and Connection are thread safe!
			 */
			Session sess = null;
			MessageProducer msgProd = null;
			try {
				// create a non-transactional session for sending messages
				sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

				// destination is our queue on JMS
				Destination dest = sess.createQueue(subject);

				// producer for sending messages
				msgProd = sess.createProducer(dest);

				// send feed in the message to the queue
				TextMessage txtMsg = sess.createTextMessage(feedDB.toString());
				msgProd.send(txtMsg);
				LOG.info("Message sent from thread '" + txtMsg.getText() + "'");

				// now we can acknowledge that the message was successfully
				// received
				// this happens when the same message is successfully sent back
				// into queue
				msg.acknowledge();
			} catch (JMSException e) {
				LOG.fatal(e.getMessage());
			} finally {
				try {
					sess.close();
					msgProd.close();
				} catch (JMSException e) {
					LOG.fatal(e.getMessage());
				}
			}
		}
	}

	/**
	 * Read feed from the specified url. Returns null if there is no content or
	 * throws Exception if there is problem with building URI.
	 * 
	 * @param url
	 * @param input
	 * @return
	 * @throws Exception
	 */
	public static SyndFeed readFeed(String url, SyndFeedInput input)
			throws Exception {
		// set various timeout to 30s
		RequestConfig requestConfig = RequestConfig.custom()
				.setConnectTimeout(30 * 1000)
				.setConnectionRequestTimeout(30 * 1000)
				.setSocketTimeout(30 * 1000).build();
		CloseableHttpClient httpClient = HttpClientBuilder.create()
				.setDefaultRequestConfig(requestConfig).build();
		SyndFeed feed = null;
		try {
			HttpGet request;

			// if exception is thrown here where we are building
			// URI then thread worker cannot continue on (throw Exception)
			try {
				URI feedUrl = new URI(url);
				request = new HttpGet(feedUrl.toString());
				request.setConfig(requestConfig);
			} catch (NullPointerException ex) {
				throw new Exception(ex);
			} catch (URISyntaxException ex) {
				throw new Exception(ex);
			} catch (IllegalArgumentException ex) {
				throw new Exception(ex);
			}

			// add headers for simulating browser request as some
			// web servers do not allow requests without header or do not work
			// properly without headers
			request.addHeader(HttpHeaders.USER_AGENT, USER_AGENT);
			request.addHeader(HttpHeaders.ACCEPT, "*/*");

			// even if execution does not succeed, catch the exception
			// here and return null. This will continue the main
			// while loop and try to get the feed again after specified seconds.
			CloseableHttpResponse response = httpClient.execute(request);

			try {
				// entity from response
				HttpEntity entity = response.getEntity();

				// build feed from entity content
				if (entity != null) {
					InputStream stream = entity.getContent();
					feed = input.build(new XmlReader(stream));
				}
			} finally {
				response.close();
			}
		} catch (IllegalStateException e) {
			LOG.error(e.getMessage());
		} catch (FeedException e) {
			LOG.error(e.getMessage());
		} catch (NoHttpResponseException e) {
			LOG.error(e.getMessage());
		} catch (ClientProtocolException e) {
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
		} finally {
			httpClient.close();
		}
		return feed;
	}

	/**
	 * Insert new feed entries into DB.
	 * 
	 * @param feed
	 * @param idList
	 * @return
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	@SuppressWarnings("unchecked")
	private void getNewEntries(SyndFeed feed,
			ArrayList<String> idList) throws IOException {
		DBObject findQuery = new BasicDBObject("feedUrl", feedDB.get("feedUrl"));
		ArrayList<SyndEntry> entries = (ArrayList<SyndEntry>) feed.getEntries();
		ArrayList<String> idListNew = new ArrayList<String>();
		
		// depending on size of entries for this feed use bulk insert or normal
		// insert. We use bulk insert with feeds that have less then or equal
		// 1000 entries.
		boolean bulkInsert = true, newEntries = false;
		if (entries.size() > 1000)
			bulkInsert = false;

		// current local entries for this feed use NORMALIZED data models using
		// One-To-Many Relationships. Reason:
		// http://blog.mongolab.com/2013/04/thinking-about-arrays-in-mongodb/
		ArrayList<DBObject> entriesDBNew = new ArrayList<DBObject>();
		for (SyndEntry entry : entries) {
			/*
			 * All elements of an item are optional, however at least one of
			 * title or description must be present. But we CANNOT TRUST USER
			 * about this. That is why we will check first for the guid, then
			 * url and then for description and the title. This combination of
			 * values will in turn be used to create a hash value using SHA-1.
			 * The hash value will represent the ID of the entry. This ID will
			 * help us determine entries that already exist. Because of possible
			 * hash collisions we use SHA-1 instead of Java hashCode.
			 */
			String id = null;
			if (entry.getUri() != null)
				// first we check if uri (guid) is available. This is the best
				// attribute as it ensures uniquely identified entry.
				id = entry.getUri();
			else if (entry.getLink() != null)
				// link is also a good unique identity
				id = entry.getLink();
			else if (entry.getDescription() != null && entry.getTitle() != null)
				// then we check if description and title are available
				id = (entry.getDescription() + entry.getTitle());
			else if (entry.getDescription() != null
					&& entry.getDescription().getValue() != null)
				// only description
				id = entry.getDescription().getValue();
			else if (entry.getTitle() != null)
				// only title
				id = entry.getTitle();

			// we absolutely need id
			if (id != null) {
				// for computing SHA-1 digest and getting it as binary data we
				// use Apache Commons DigestUtil
				String idHash = DigestUtils.sha1Hex(id);

				if (!idList.contains(idHash)) {
					// TODO: Check for simmilarity between other id's of other
					// entries using Levensthein distance.

					// TODO: If similarity between id's is not found then also
					// check for similarity between full page content using Jaccard
					// distance.

					// does not exist yet, save it to DB we cannot set it to _id
					// (ObjectId) as it only supports 24hex or 96bits where the
					// SHA-1 produces 160bits hash
					BasicDBObject entryDBNew = new BasicDBObject("idHash",
							idHash);
					// save raw id for computing similarity
					entryDBNew.append("idRaw", id);

					if (entry.getTitle() != null)
						entryDBNew.append("title", entry.getTitle());
					if (entry.getLink() != null) {
						entryDBNew.append("link", entry.getLink());

						// if link exists we can fetch the whole entry (HTML
						// page) using Apache HttpComponents library, module
						// HttpClient
						String webPage = fetchWebPage(entry);

						// add to database
						entryDBNew.append("fullContent", webPage);
					}
					if (entry.getDescription() != null)
						entryDBNew.append("description", entry.getDescription()
								.getValue());
					if (entry.getAuthors() != null
							&& entry.getAuthors().size() > 0) {
						// call method for constructing the list of authors for
						// DB
						ArrayList<BasicDBObject> authors = getAuthors((ArrayList<SyndPerson>) entry
								.getAuthors());
						feedDB.put("authors", authors);
						entryDBNew.put("authors", authors);
					}
					if (entry.getCategories() != null
							&& entry.getCategories().size() > 0) {
						ArrayList<BasicDBObject> categories = getCategories((ArrayList<SyndCategory>) entry
								.getCategories());
						entryDBNew.append("categories", categories);
					}
					// comments does not exist
					if (entry.getEnclosures() != null
							&& entry.getEnclosures().size() > 0) {
						ArrayList<BasicDBObject> enclosures = new ArrayList<BasicDBObject>();
						for (SyndEnclosure enclosure : (ArrayList<SyndEnclosure>) entry
								.getEnclosures()) {
							BasicDBObject enclosureDB = new BasicDBObject();

							// all three attributes are required but we cannot
							// trust the user
							if (enclosure.getUrl() != null)
								enclosureDB.append("url", enclosure.getUrl());
							if (enclosure.getLength() != 0)
								enclosureDB.append("length",
										enclosure.getLength());
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
					// source does not exist in rome library
					newEntries = true;
					
					// depending on type of insert
					if (bulkInsert) {
						// insert new entry into list
						entriesDBNew.add(entryDBNew);
	
						// add id to old list and new list
						idList.add(idHash);
						idListNew.add(idHash);
					} else {
						// large feed or lots of entries. Do a normal insert for each entry.
						// insert new entry
						entriesColl.insert(entryDBNew);

						// push new hash id into entries for current feed
						DBObject newHash = new BasicDBObject("entries", idHash);
						DBObject updateQuery = new BasicDBObject("$push", newHash);
						rssColl.update(findQuery, updateQuery);
					}
				}
			}
		}
		
		if (bulkInsert && newEntries) {
			// bulk insert new entries
			entriesColl.insert(entriesDBNew);
			
			// push all new references to entries
			DBObject idHashes = new BasicDBObject("$each", idListNew);
			DBObject each = new BasicDBObject("entries", idHashes);
			DBObject updateQuery = new BasicDBObject("$push", each);
			rssColl.update(findQuery, updateQuery);
		}
	}

	/**
	 * Fetch web page given the Synd entry and return it. For read we use Apache
	 * HttpComponents library, module HttpClient.
	 * 
	 * @param entry
	 * @return
	 * @throws IOException
	 */
	private String fetchWebPage(SyndEntry entry) {
		// set various timeout to 30s
		RequestConfig requestConfig = RequestConfig.custom()
				.setConnectTimeout(30 * 1000)
				.setConnectionRequestTimeout(30 * 1000)
				.setSocketTimeout(30 * 1000).build();
		CloseableHttpClient httpClient = HttpClientBuilder.create()
				.setDefaultRequestConfig(requestConfig).build();
		String webPage = null;
		try {
			if (entry.getLink() != null) {
				URI uri = new URI(entry.getLink());
				HttpGet httpGet = new HttpGet(uri.toString());
				httpGet.setConfig(requestConfig);

				// add header for simulating browser request as some
				// web servers block automatic querying
				httpGet.addHeader(HttpHeaders.USER_AGENT, USER_AGENT);
				CloseableHttpResponse response = httpClient.execute(httpGet);

				try {
					// entity from response
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						BufferedReader in = new BufferedReader(
								new InputStreamReader(entity.getContent()));
						String inputLine;
						StringBuffer strResponse = new StringBuffer();

						// read response
						while ((inputLine = in.readLine()) != null) {
							strResponse.append(inputLine);
						}
						in.close();
						webPage = strResponse.toString();
					}
				} finally {
					response.close();
				}
			}
		} catch (URISyntaxException e) {
			LOG.error(e.getMessage());
		} catch (IllegalArgumentException e) {
			LOG.error(e.getMessage());
		} catch (NoHttpResponseException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
		} finally {
			try {
				httpClient.close();
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		}

		return webPage;
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
		// this field is for RSS Delegate worker to check for stalled threads or
		// threads that crashed and were not able to update used filed back to 0
		feedDB.put("accessedAt", new Date());

		/******** REQUIRED channel elements as defined in RSS 2.0 Specification ********/
		// http://cyber.law.harvard.edu/rss/rss.html#
		// even though this elements are required by specification we
		// cannot trust the source
		if (feed.getTitle() != null)
			feedDB.put("title", feed.getTitle());
		if (feed.getLink() != null)
			feedDB.put("link", feed.getLink());
		if (feed.getDescription() != null)
			feedDB.put("description", feed.getDescription());

		/******** OPTIONAL channel elements as defined in RSS 2.0 Specification ********/
		if (feed.getLanguage() != null)
			feedDB.put("language", feed.getLanguage());
		if (feed.getCopyright() != null)
			feedDB.put("copyright", feed.getCopyright());
		// no specific get method for managing editor and web master in rome
		// library. Using authors instead
		if (feed.getAuthors() != null && feed.getAuthors().size() > 0) {
			ArrayList<BasicDBObject> authors = getAuthors((ArrayList<SyndPerson>) feed
					.getAuthors());
			feedDB.put("authors", authors);
		}
		if (feed.getPublishedDate() != null)
			feedDB.put("pubDate", feed.getPublishedDate());
		// last build date does not exist in rome library
		if (feed.getCategories() != null && feed.getCategories().size() > 0) {
			ArrayList<BasicDBObject> categories = getCategories((ArrayList<SyndCategory>) feed
					.getCategories());
			feedDB.put("category", categories);
		}
		// generator does not exist in rome library
		// docs does not exist in rome library
		// cloud does not exist in rome library
		// ttl does not exist in rome library
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
			if (feedImage.getUrl() != null || feedImage.getTitle() != null
					|| feedImage.getLink() != null
					|| feedImage.getDescription() != null)
				feedDB.put("image", imageDB);
			// width does not exist in rome library
			// height does not exist in rome library
		}
		// rating does not exist in rome library
		// text input does not exist in rome library
		// skip hours does not exist in rome library
		// skip days does not exist in rome library

		rssColl.update(new BasicDBObject("feedUrl", feedDB.get("feedUrl")),
				feedDB);
	}

	/**
	 * Returns a list of categories for feed or entry.
	 * 
	 * @param categories
	 * @return
	 */
	private ArrayList<BasicDBObject> getCategories(
			ArrayList<SyndCategory> categories) {
		ArrayList<BasicDBObject> categoriesDB = new ArrayList<BasicDBObject>();
		for (SyndCategory category : categories) {
			BasicDBObject categoryDB = new BasicDBObject();

			if (category.getName() != null)
				categoryDB.append("name", category.getName());
			if (category.getTaxonomyUri() != null)
				categoryDB.append("taxonomyURI", category.getTaxonomyUri());
			if (category.getName() != null || category.getTaxonomyUri() != null)
				categoriesDB.add(categoryDB);
		}

		return categoriesDB;
	}

	/**
	 * Returns a list of authors for feed or entry.
	 * 
	 * @param authors
	 * @return
	 */
	private static ArrayList<BasicDBObject> getAuthors(
			ArrayList<SyndPerson> authors) {
		ArrayList<BasicDBObject> authorsDB = new ArrayList<BasicDBObject>();
		for (SyndPerson author : authors) {
			BasicDBObject authorDB = new BasicDBObject();

			if (author.getName() != null)
				authorDB.append("name", author.getName());
			if (author.getUri() != null)
				authorDB.append("uri", author.getUri());
			if (author.getName() != null || author.getUri() != null)
				authorsDB.add(authorDB);
		}

		return authorsDB;
	}

}
