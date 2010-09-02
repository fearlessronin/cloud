package cloudbase;


import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Instance;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;


public class ScanClient {
	public static void main(String[] args) throws CBException, CBSecurityException,
	TableNotFoundException {
		//enter instance name
		String instanceName = "myinstance";
		String zkServerNames = "localhost";
		Instance inst = new ZooKeeperInstance(instanceName,zkServerNames);

		//--- enter user name and password
		Connector conn = new Connector(inst,"user", "password".getBytes());

		Authorizations auths = new Authorizations();

		// -- create scanner
		Scanner scanner = conn.createScanner("enron", auths);

		// -- fetch a column family
		scanner.fetchColumnFamily(new Text("from"));

		// --- iterate over entries
		for(Entry<Key, Value> entry : scanner) {
			// -- print out row and value
			System.out.println(entry.getKey().getRow() + "\t" + entry.getValue().toString());
		}
	}
}
