package server;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

public class CassandraTables implements Replicable {
	protected static final Logger log = Logger.getLogger(CassandraTables.class.getName());

	/**
	 * CQLSH is path to canssandra cqlsh. To change this path, don't change the
	 * default value below but instead use the system property
	 * -Dcqlsh=/path/to/cqlsh as an argument to gpServer.sh.
	 *
	 */
	private final static String CQLSH = System.getProperty("cqlsh") != null ? System.getProperty("cqlsh")
			: "/home/ubuntu/apache-cassandra-3.11.1/";
	public final static String DEFAULT_KEYSPACE = System.getProperty("keyspace")
			!=null ?
			System.getProperty("keyspace")
			: "demo";
	private final static String DEFAULT_TABLE = "table";

	private final static String DEFAULT_DATA_FILE = "data";

	int id;
	Cluster cluster;
	Session session;

	private static int idGen = 0;

    /**
     * Generate a new Cluster ID
     */
    private static int newID() {
        return idGen++;
    }

	/**
	 * @param name The table name
	 * @return File Name that holds the checkpoint
	 */
	@Override
	public String checkpoint(String name) {
		// Dump data without drop the table entirely. DON'T forget to quote the file
		// name with apostrophe
		String fileName = DEFAULT_DATA_FILE + id + ".csv";
		Process proc;
		// String tableName = name+"-"+id;
		String tableName = name;
		try {
			proc = Runtime.getRuntime()
					.exec(new String[] { CQLSH, "-e", "COPY " + tableName + " TO '" + fileName +"';" });
			proc.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return fileName;
	}

	/**
	 * 
	 */
	@Override
	public boolean execute(Request request) {		
		// execute request here
		if (request instanceof RequestPacket) {
			String resultValue = "";
			String requestValue = ((RequestPacket) request).requestValue;

			// Cassandra Specific Implementation
			resultValue = executeQuery(requestValue);

			((RequestPacket) request).setResponse("Execution Results: [" +
				resultValue + "]");
		} 
		else System.err.println("Unknown packet type: " + request.getSummary());
		return true;
	}
	
	/**
	 * no need to implement this method, implement the execute above
	 */
	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		// execute request without replying back to client

		// identical to above unless app manages its own messaging
		return this.execute(request);
	}

	/**
	 * @param name The table name
	 * @param state The file name that holds the checkpoint
	 */
	@Override
	public boolean restore(String name, String state) {
		Process proc;
		String tableName = name;
		try {
			proc = Runtime.getRuntime()
					.exec(new String[] { CQLSH, "-e", "COPY " + tableName + " FROM '" + state +"';" });
			proc.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * No need to implement unless you want to implement your own
	 * packet types (optional).
	 */
	@Override
	public Request getRequest(String req) throws RequestParseException {
		return null;
	}

	/**
	 * No need to implement unless you want to implement your own
	 * packet types (optional).
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return null;
	}

	/**
     * Execute he query on Cassandra
     * @param command command to be executed
     * @return the results from the cluster
     */
    protected String executeQuery(String command) {
        // Execute and get the result
        ResultSet results = null;
        try {
            results = session.execute(command);
        } catch (Exception e) {
            log.info("Execution failed.");
            return e.getLocalizedMessage();
        }
        
        log.info("Execution successful.");
        // Get results
        return results.all().toString();
        // Return requests
        // return command+"=>"+results.all().toString();
    }

	public CassandraTables() {
		super();
		// Connect to the cluster and keyspace "demo"
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect(DEFAULT_KEYSPACE);
		id = newID(); 
	}

}
