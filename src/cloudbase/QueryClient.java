package cloudbase;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Instance;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;


public class QueryClient {
	public static void main(String[] args) throws CBException,CBSecurityException,TableNotFoundException {
		//enter instance name
		String instanceName = "myinstance";
		String zkServerNames = "localhost";
		Instance inst = new ZooKeeperInstance(instanceName,zkServerNames);

		//--- enter user name and password
		Connector conn = new Connector(inst,"user", "password".getBytes());

		Authorizations auths = new Authorizations();

		// -- scan the index for a given term
		Scanner indexScanner = conn.createScanner("enron_text_index", auths);
		Text term = new Text(args[0]);
		indexScanner.setRange(new Range(term,term));

		// -- create ranges for each rowID
		ArrayList<Range> ranges = new ArrayList<Range>();
		for (Entry<Key, Value> entry : indexScanner) {
			Text rowID = entry.getKey().getColumnQualifier();
			ranges.add(new Range(rowID, rowID));
		}

		// -- create batch scanner
		BatchScanner bscan = conn.createBatchScanner("enron",auths,10);

		// -- set ranges
		bscan.setRanges(ranges);

		//-- select From column family
		bscan.fetchColumnFamily(new Text("from"));

		// -- iterate over batch scanner entries and print out 'from' field
		for(Entry<Key,Value> entry : bscan)
			System.out.println(entry.getValue().toString());
	
		// -- close batch scanner
		bscan.close();
	}
}
