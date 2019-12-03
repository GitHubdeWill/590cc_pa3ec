package client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosClientAsync;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Callback;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.examples.linwrites.SimpleAppRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

/**
 * Use PaxosClientAsync or ReconfigurableClientAsync as in the tutorial
 * examples.
 */
public class CassandraTablesClient extends ReconfigurableAppClientAsync<SimpleAppRequest> {

	/**
	 * ` default keyspace
	 */
	public final static String DEFAULT_KEYSPACE = System.getProperty("keyspace") != null
			? System.getProperty("keyspace")
			: "demo";

	/**
	 * Make a request contains the
	 * 
	 * @param command
	 * @return
	 */
	public static SimpleAppRequest makeCassandraCommand(String command) {
		return new SimpleAppRequest(PaxosConfig.getDefaultServiceName(), command,
				SimpleAppRequest.PacketType.COORDINATED_WRITE);
	}

	public CassandraTablesClient() throws IOException {
		super();
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		for (String s : args) {
			print(s);
		}
		InetSocketAddress isa = args.length > 0 ? Util.getInetSocketAddressFromString(args[0]) : null;
		int startPosition = 0;
		if (isa == null) {
			startPosition = 1;
		}
		CassandraTablesClient cassandraTablesClient = new CassandraTablesClient();

		// Send all commands supplied in the arguments
		for (int i = startPosition; i < args.length; i++) {
			SimpleAppRequest request;
			cassandraTablesClient.sendRequest(request = CassandraTablesClient.makeCassandraCommand(args[i]),
			new Callback<Request, SimpleAppRequest>() {
				long createTime = System.currentTimeMillis();
				@Override
				public SimpleAppRequest processResponse(Request response) {
					assert(response instanceof SimpleAppRequest) :
							response.getSummary();
					System.out
							.println(0+": Response for request ["
									+ request.getSummary()
									+ "] = "
									+ ((SimpleAppRequest)response).getValue()
									+ " received in "
									+ (System.currentTimeMillis() - createTime)
									+ "ms");
					return (SimpleAppRequest) response;
				}
			});
		}
	}

	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		try {
			return new SimpleAppRequest(new JSONObject(stringified));
		} catch (JSONException je) {
			throw new RequestParseException(je);
		}
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>(Arrays.asList(SimpleAppRequest.PacketType.values()));
	}

	public static void print(Object o) {
		System.out.print(o);
	}

}
