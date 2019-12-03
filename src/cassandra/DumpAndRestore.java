package cassandra;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author gaozy
 *
 */
public class DumpAndRestore {
	/**
	 * Substitute this with the path to your cqlsh to make it work
	 */
	private final static String CQLSH = "/Users/gaozy/Documents/CS590-cloud computing/PA2/apache-cassandra-3.0.15/bin/cqlsh";

	private final static String DEFAULT_KEYSPACE = "demo";
	private final static String DEFAULT_TABLE = "CassandraTables0";

	private final static String DEFAULT_DATA_FILE = "data.csv";

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException {

		String tableName = DEFAULT_KEYSPACE+"."+DEFAULT_TABLE;

		Cluster cluster;
		Session session;

		// Connect to the cluster and keyspace "demo"
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect(DEFAULT_KEYSPACE);

		// Create table users
		session.execute("create table if not exists "+ tableName
				+" (id UUID PRIMARY KEY, lastname text, firstname text)");

		// Insert a few records into the table
		session.execute("INSERT INTO "+tableName+" (id, lastname, firstname) VALUES (5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 'VOS','Marianne');");
		session.execute("INSERT INTO "+tableName+" (id, lastname, firstname) VALUES (e7cd5752-bc0d-4157-a80f-7523add8dbcd, 'VAN DER BREGGEN','Anna');");
		session.execute("INSERT INTO "+tableName+" (id, lastname, firstname) VALUES (e7ae5cf3-d358-4d99-b900-85902fda9bb0, 'FRAME','Alex');");
		session.execute("INSERT INTO "+tableName+" (id, lastname, firstname) VALUES (220844bf-4860-49d6-9a4b-6b5d3a79cbfb, 'TIRALONGO','Paolo');");
		session.execute("INSERT INTO "+tableName+" (id, lastname, firstname) VALUES (6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'KRUIKSWIJK','Steven');");
		session.execute("INSERT INTO "+tableName+" (id, lastname, firstname) VALUES (fb372533-eb95-4bb4-8685-6ef61e994caa, 'MATTHEWS', 'Michael');");

		// Show the content in the table
		ResultSet result = session.execute("SELECT * from "+tableName);
		System.out.println("Command: SELECT * from "+tableName);
		for(Row row : result){
			System.out.println(row.toString());
		}

		// Dump data without drop the table entirely. DON'T forget to quote the file name with apostrophe
		Process proc = Runtime.getRuntime().exec(new String[]{CQLSH, "-e", "COPY "+tableName+" TO '"+DEFAULT_DATA_FILE+"';"});
		proc.waitFor();

		// Clear the table
		session.execute("TRUNCATE "+tableName+";");

		// Show that the table has been cleared
		result = session.execute("SELECT * from "+tableName);
		System.out.println("Command: SELECT * from "+tableName);
		for(Row row : result){
			System.out.println(row.toString());
		}

		// Restore data from the file
		proc = Runtime.getRuntime().exec(new String[]{CQLSH, "-e", "COPY "+tableName+" from '"+DEFAULT_DATA_FILE+"';"});
		proc.waitFor();

		// Show that the data has been restored from the file
		result = session.execute("SELECT * from "+tableName);
		System.out.println("Command: SELECT * from "+tableName);
		for(Row row : result){
			System.out.println(row.toString());
		}

		// Drop table users
		session.execute("drop table users");

		// Delete the file
		Files.deleteIfExists(Paths.get(DEFAULT_DATA_FILE));


		// Clean up the connection by closing it
		cluster.close();
	}
}
