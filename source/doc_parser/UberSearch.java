import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.LogManager;
import org.bson.Document;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class UberSearch {

	public static int DEFAULT_MAX_THREADS = 10;
	public static final long SEC_IN_MS = 1000;
	public static final long MIN_IN_MS = 60 * SEC_IN_MS;
	public static final long HOUR_IN_MS = 60 * MIN_IN_MS;
	public static final long DAY_IN_MS = 24 * HOUR_IN_MS;
	public static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	//Cassandra properties
	public static final String CASSANDRA_HOST = "localhost";
	public static final String CASSANDRA_KEY_SPACE = "crawler";

	public static List<Integer> threadCompletionList = new ArrayList<Integer>();
	public static Hashtable<String, Document> matchesFound = new Hashtable<String, Document>();

	public static MongoDatabase getMongoDatabase() {
		MongoClient mongoClient = new MongoClient( "localhost",27017);
		MongoDatabase database = mongoClient.getDatabase("cdsearchdb");
		return database;
	}

	public static void uberSearch(String searchTerm, int threadCount, int docsCount) throws Exception {
		MongoDatabase database = getMongoDatabase();
		
		int totalDocsCount = (int)database.getCollection("webpages").count();
		
		if(threadCount == -1) {
			threadCount = DEFAULT_MAX_THREADS;
		}
		if(docsCount == -1) {
			docsCount = totalDocsCount;
		}

		int processBlockCount = docsCount / threadCount;
		Timestamp startTime = getCurrentTimestamp();
		
		//check if search exists already
		if(displayPriorSearchIfExists(searchTerm)) {
			System.out.println("Completed prior search results display.");
			return;
		}

		long range = 1234567L;
		Random r = new Random();
		String searchId = (long)(r.nextDouble()*range)+"";

		for(int ii = 1; ii <= threadCount; ii++) {			
			blockSearch(ii, processBlockCount, searchTerm);			
		}
		System.out.println("Started all threads");
		int count = 1;
		while(threadCompletionList.size() != threadCount) {
			count += 1;
			Thread.currentThread().sleep(1000);
			if(count == 5) {
				System.out.println("Searching....");
				count = 1;
			}
		}

		String timeTaken = formatElapsedTime(startTime.getTime());
		//update search MetaData
		updateSearchMetaData(searchId, searchTerm, matchesFound.size());

		//update search references
		updateSearchReferences(searchId);
		
		//display search if new search
		Set<String> keys = matchesFound.keySet();
		for(String key: keys){
			System.out.println("URL "+URLDecoder.decode(key, "UTF-8"));
		}
		System.out.println(" ");
		System.out.println("################");
		System.out.println(" ");
		System.out.println("Search Term :"+ searchTerm);
		System.out.println("Total number of docs in database :"+ totalDocsCount);
		System.out.println("Number of documents searched :"+docsCount);
		System.out.println("Number of threads :"+threadCount);
		System.out.println("Number of Docs searched by each thread :"+processBlockCount);
		System.out.println(" ");
		System.out.println("Matches found :"+matchesFound.size());
		System.out.println("Total Time taken "+timeTaken);
		System.out.println(" ");
		System.out.println("################");

	}

	public static void blockSearch(final int threadOrder, final int processBlock, final String searchTerm)  {		
		Runnable r = new Runnable() {
			MongoDatabase database = null;
			public void run() {
				try {
					database = getMongoDatabase();
					int cursorStart = (threadOrder - 1) * processBlock;
					FindIterable<Document> docs= database.getCollection("webpages").find().skip(cursorStart).limit(processBlock);
					String url = "";
					boolean isAndSearch = false;
					boolean isOrSearch = false;
					if(searchTerm.equalsIgnoreCase(" and ")) {
						isAndSearch = true;

					}
					if(searchTerm.equalsIgnoreCase(" or ")) {
						isOrSearch = true;
					}						
					for(Document doc: docs) {						
						if(((String)doc.get("html")).contains(searchTerm)) {
							url = (String)doc.get("raw_url");
							try {
								URI uri = new URI(url);
								url = URLEncoder.encode(uri.getHost(), "UTF-8");
							} catch(Exception ex) {
								//continue without encoding								
							}							
							if(!matchesFound.containsKey(url)) {
								matchesFound.put(url, doc);
							}
						}	    			
					}
				} catch(Exception ex) {
					ex.printStackTrace();
				} finally {
					System.out.println("Thread #"+threadOrder+" done.");
					updateProcessMointor(threadOrder);
				}
			}
		};	
		ExecutorService executor = Executors.newCachedThreadPool();
		executor.submit(r);
		executor.shutdown();
	}

	public static void updateProcessMointor(int threadOrder) {
		threadCompletionList.add(new Integer(threadOrder));
	}

	public static String formatElapsedTime(long startTime){
		long elapsedTime = System.currentTimeMillis() - startTime;

		int days = (int)(elapsedTime / DAY_IN_MS);
		elapsedTime = elapsedTime % DAY_IN_MS;
		int hours = (int)(elapsedTime / HOUR_IN_MS);
		elapsedTime = elapsedTime % HOUR_IN_MS;
		int mins = (int)(elapsedTime / MIN_IN_MS);
		elapsedTime = elapsedTime % MIN_IN_MS;
		int secs = (int)(elapsedTime / SEC_IN_MS);
		elapsedTime = elapsedTime % SEC_IN_MS;

		StringBuffer sb = new StringBuffer();
		if (days != 0) sb.append("days: "+ days + ", ");
		if (hours != 0) sb.append("hrs: "+ hours + ", ");
		if (mins != 0) sb.append("min: "+ mins + ", ");
		if (secs != 0) sb.append("sec: "+ secs + ", ");
		if (elapsedTime != 0) sb.append("msec: "+ elapsedTime);
		return sb.toString();
	}


	public static java.sql.Timestamp getCurrentTimestamp() {
		try {
			java.sql.Timestamp currentTimestamp;
			SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT);
			Calendar calendar = Calendar.getInstance();
			String formattedDate = dateFormat.format(calendar.getTime());
			java.util.Date currentDate = dateFormat.parse(formattedDate);
			currentTimestamp = new java.sql.Timestamp(currentDate.getTime());
			return currentTimestamp;
		} catch(Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	public static void updateSearchMetaData(String searchId, String searchTerm, int matchesFound) {
		Cluster cluster = null;
		Session session = null;

		try {
			System.out.println("Updating search_meta_data table");
			cluster = Cluster.builder().addContactPoints(CASSANDRA_HOST).build();
			session = cluster.connect(CASSANDRA_KEY_SPACE);	

			PreparedStatement prepared = session.prepare("INSERT INTO crawler.SEARCH_META_DATA(search_id, search_term, matches_found, date_created) values (?, ? ,? ,?)");
			BoundStatement bound = prepared.bind()
					.setString(0, searchId)
					.setString(1, searchTerm)
					.setString(2, matchesFound+"")
					.setTimestamp(3, getCurrentTimestamp());
			session.execute(bound);
		} catch(Exception ex) {

		} finally {
			try {
				session.close();
			} catch(Exception ex) {

			}
			try {
				cluster.close();
			} catch(Exception ex) {

			}		
		}
	}
	
	public static void updateSearchReferences(String searchId) {
		Cluster cluster = null;
		Session session = null;

		try {
			System.out.println("updating search_references table");
			cluster = Cluster.builder().addContactPoints(CASSANDRA_HOST).build();
			session = cluster.connect(CASSANDRA_KEY_SPACE);	
			Set<String> keys = matchesFound.keySet();
			int counter = 0;
			for(String key: keys){
				Document doc = matchesFound.get(key);
				counter += 1;
				PreparedStatement prepared = session.prepare("INSERT INTO crawler.search_references(search_reference_id, html, search_id, source_ref_id, url, date_created) values (?, ? ,? ,?, ?, ?)");
				BoundStatement bound = prepared.bind()
						.setString(0, searchId+"-"+counter)
						.setString(1, (String)doc.get("html"))
						.setString(2, searchId)
						.setString(3, (String)doc.get("_id"))
						.setString(4, (String)doc.get("raw_url"))
						.setTimestamp(5, getCurrentTimestamp());
				session.execute(bound);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				session.close();
			} catch(Exception ex) {

			}
			try {
				cluster.close();
			} catch(Exception ex) {

			}		
		}
	}
	
	public static boolean displayPriorSearchIfExists(String searchTerm) {
		Cluster cluster = null;
		Session session = null;
		try {
			System.out.println("Checking if prior search exists");

			cluster = Cluster.builder().addContactPoints(CASSANDRA_HOST).build();
			session = cluster.connect(CASSANDRA_KEY_SPACE);			
			String cqlStatement = "SELECT search_id FROM crawler.SEARCH_META_DATA where search_term='"+searchTerm+"'";
			ResultSet rs = session.execute(cqlStatement);
			if(rs.isExhausted()) {
				return false;
			}
			
			for (Row row : rs ) {
				System.out.println("Matching Search Id : "+row.getString(0));
				cqlStatement = "SELECT url FROM crawler.search_references where search_id='"+row.getString(0)+"'";
				ResultSet rsInner = session.execute(cqlStatement);
				if(rsInner.isExhausted()) {
					return false;
				}
				System.out.println("################");
				for (Row rowInner : rsInner) {
					System.out.println(rowInner.getString(0));
				}
				System.out.println("################");
			}
			
			return true;
		} catch(Exception ex) {
			ex.printStackTrace();
			return false;
		} finally {
			try {
				session.close();
			} catch(Exception ex) {

			}
			try {
				cluster.close();
			} catch(Exception ex) {

			}		
		}
	}
	
	public static void main(String args[]) {
		try {
			//turn off logging
			/*LogManager.getLogger("org.mongodb.driver.connection").setLevel(org.apache.log4j.Level.OFF);
	        LogManager.getLogger("org.mongodb.driver.management").setLevel(org.apache.log4j.Level.OFF);
	        LogManager.getLogger("org.mongodb.driver.cluster").setLevel(org.apache.log4j.Level.OFF);
	        LogManager.getLogger("org.mongodb.driver.protocol.insert").setLevel(org.apache.log4j.Level.OFF);
	        LogManager.getLogger("org.mongodb.driver.protocol.query").setLevel(org.apache.log4j.Level.OFF);
	        LogManager.getLogger("org.mongodb.driver.protocol.update").setLevel(org.apache.log4j.Level.OFF);
	        LogManager.getLogger("com.mongodb.diagnostics.logging.JULLogger").setLevel(org.apache.log4j.Level.OFF);*/
	        LogManager.getLogger("org.mongodb").setLevel(org.apache.log4j.Level.OFF);
	        
			if(args.length != 1) {
				System.out.println("Incorrect Usage : UberSearch <search term>");
			}
			String searchTerm = args[0];
			int threadCount = -1;
			int docsCount = -1;
			if(args.length >= 2) {
				threadCount = Integer.parseInt(args[1]);
			}
			if(args.length == 3) {
				docsCount = Integer.parseInt(args[2]);
			}
			uberSearch(searchTerm, threadCount, docsCount);
			//displayPriorSearchIfExists(searchTerm);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Search Failed.");
		}
	}

}
