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

public class MergeSearch {

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

	public static void mergeSearchStatus(String searchId, String searchTerm) throws Exception {
		Cluster cluster = null;
		Session session = null;
		try {
			cluster = Cluster.builder().addContactPoints(CASSANDRA_HOST).build();
			session = cluster.connect(CASSANDRA_KEY_SPACE);			
			int count = 0;			
			long completedThreadCount = 0;
			while(completedThreadCount != DEFAULT_MAX_THREADS) {
				count += 1;
				Thread.currentThread().sleep(1000);
				completedThreadCount = getThreadCount(cluster, session, searchId);

				if(count == 5) {
					System.out.println("Searching....");
					count = 1;
				}
			}
			long matchCount = getMatchCount(cluster, session, searchId);
			System.out.println("Total time taken : "+getTimeTaken(cluster, session, searchId));
			updateSearchMetaData(searchId, searchTerm, matchCount);
			String timeTaken = getTimeTaken(cluster, session, searchId)+" seconds";
			
			System.out.println(" ");
			System.out.println("################");
			System.out.println(" ");
			System.out.println("Search Term :"+ searchTerm);
			System.out.println(" ");
			System.out.println("Matches found :"+matchCount);
			System.out.println("Total Time taken "+timeTaken);
			System.out.println(" ");
			System.out.println("################");

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

	public static void updateSearchMetaData(String searchId, String searchTerm, long matchesFound) {
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


	public static long getThreadCount(Cluster cluster, Session session, String searchId) {
		cluster = Cluster.builder().addContactPoints(CASSANDRA_HOST).build();
		session = cluster.connect(CASSANDRA_KEY_SPACE);			
		String cqlStatement = "SELECT count(*) FROM crawler.SEARCH_THREAD_STATUS where search_id='"+searchId+"'";
		ResultSet rs = session.execute(cqlStatement);
		return rs.one().getLong(0);
	}
	
	public static long getMatchCount(Cluster cluster, Session session, String searchId) {
		cluster = Cluster.builder().addContactPoints(CASSANDRA_HOST).build();
		session = cluster.connect(CASSANDRA_KEY_SPACE);			
		String cqlStatement = "SELECT count(*) FROM crawler.search_references where search_id='"+searchId+"'";
		ResultSet rs = session.execute(cqlStatement);
		return rs.one().getLong(0);
	}

	public static long getTimeTaken(Cluster cluster, Session session, String searchId) {
		cluster = Cluster.builder().addContactPoints(CASSANDRA_HOST).build();
		session = cluster.connect(CASSANDRA_KEY_SPACE);			
		String cqlStatement = "SELECT time_taken FROM crawler.SEARCH_THREAD_STATUS where search_id='"+searchId+"'";
		ResultSet rs = session.execute(cqlStatement);
		if(rs.isExhausted()) {
			throw new RuntimeException();
		}
		long timeTaken = 0;
		for (Row row : rs ) {
			timeTaken = timeTaken + row.getLong(0);
		}
		return timeTaken;
	}

	public static void main(String args[]) {
		try {
			LogManager.getLogger("org.mongodb").setLevel(org.apache.log4j.Level.OFF);

			if(args.length != 2) {
				System.out.println("Incorrect Usage : MergeSearch <search_id> <search term>");
			}
			String searchId = args[0];
			String searchTerm = args[1];
			mergeSearchStatus(searchId, searchTerm);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Search Failed.");
		}
	}

}
