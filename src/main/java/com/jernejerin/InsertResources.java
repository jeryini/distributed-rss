package com.jernejerin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

/**
 * The InsertResources program writes all RSS sources i.e. feeds from csv file
 * to MongoDB. The user can specify command line arguments for setting host,
 * port, dbName, collName and filePath.
 * 
 * @author Jernej Jerin
 * @version 1.0
 * @since 2014-05-06
 */
public class InsertResources {

	/** The default database's host address. */
	private static String host = "localhost";

	/** The default port on which the database is running. */
	private static int port = 27017;

	/** The name of the database to use. */
	private static String dbName = "rssdb";

	/** The name of the collection to use. */
	private static String collName = "feeds";

	/** The path to the file with RSS feeds. */
	private static String filePath = "./10K-RSS-feeds.csv";

	// logger for this class
	private static final Logger LOG = Logger.getLogger(InsertResources.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		BufferedReader br = null;
		MongoClient mongoClient = null;
		try {
			// configure logger
			props.load(new FileInputStream("log4j.properties"));
			PropertyConfigurator.configure(props);

			// create Options object
			Options options = new Options();

			// add options
			options.addOption("help", false, "help for usage");
			options.addOption("host", true, "database's host address");
			options.addOption("port", true,
					"port on which the database is running");
			options.addOption("dbName", true, "the name of the database to use");
			options.addOption("collName", true, "the name of collection to use");
			options.addOption("filePath", true,
					"the path of the file with RSS feeds");

			// parser for command line arguments
			CommandLineParser parser = new GnuParser();
			CommandLine cmd = parser.parse(options, args);
			
			if (cmd.hasOption("help")) {
				HelpFormatter help = new HelpFormatter();
				help.printHelp("java -jar InsertResources", options);
				System.exit(-1);
			}
			if (cmd.getOptionValue("host") != null)
				host = cmd.getOptionValue("host");
			if (cmd.getOptionValue("port") != null)
				port = Integer.parseInt(cmd.getOptionValue("port"));
			if (cmd.getOptionValue("dbName") != null)
				dbName = cmd.getOptionValue("dbName");
			if (cmd.getOptionValue("collName") != null)
				collName = cmd.getOptionValue("collName");
			if (cmd.getOptionValue("filePath") != null)
				filePath = cmd.getOptionValue("filePath");

			mongoClient = new MongoClient(host, port);
			DB rssDB = mongoClient.getDB(dbName);
			DBCollection rssColl = rssDB.getCollection(collName);
			LOG.info("Opened connection to MongoDB");

			// purge DB
			rssColl.remove(new BasicDBObject());

			br = new BufferedReader(new FileReader(filePath));

			String line;
			while ((line = br.readLine()) != null) {
				// save each feed into DB
				BasicDBObject feedNew = new BasicDBObject("feedUrl", line);
				rssColl.insert(feedNew);
				LOG.info("Inserted new feed into DB.");
			}

			// create index on "accessedAt", ascending
			rssColl.createIndex(new BasicDBObject("accessedAt", 1));

			br.close();
		} catch (UnknownHostException e) {
			LOG.fatal(e.getMessage());
		} catch (MongoException e) {
			LOG.fatal(e.getMessage());
		} catch (FileNotFoundException e) {
			LOG.fatal(e.getMessage());
		} catch (IOException e) {
			LOG.fatal(e.getMessage());
		} catch (ParseException e) {
			LOG.fatal(e.getMessage());
		} finally {
			mongoClient.close();
			LOG.info("Closed connection to MongoDB");
		}
	}
}
