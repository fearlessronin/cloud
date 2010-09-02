package cloudbase;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.Text;

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Instance;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;

public class IngestClient {
	public static void main(String[] args) throws CBException, CBSecurityException,TableNotFoundException, IOException {
		//enter instance name
		String instanceName = "myinstance";
		String zkServerNames = "localhost";
		Instance inst = new ZooKeeperInstance(instanceName,zkServerNames);

		//--- enter user name and password
		Connector conn = new Connector(inst,"user", "password".getBytes());

		// -- create writer
		BatchWriter writer = conn.createBatchWriter("enron",100000L,1000L,10);

		// -- enter raw filename
		BufferedReader reader = new BufferedReader(new FileReader(".txt"));

		String[] fieldNameStrings = reader.readLine().split("\\t");
		Text[] columnFamilies = new Text[fieldNameStrings.length];
		for(int i=0; i < fieldNameStrings.length; i++)
			columnFamilies[i] = new Text(fieldNameStrings[i]);

		String line = reader.readLine();
		while(line != null) {
			String[] fields = line.split("\\t");

			if(fields.length < 4) {
				line = reader.readLine();
				continue;
			}
		
		// ---- create mutation
		Mutation m = new Mutation(new Text(UUID.randomUUID().toString()));

		for(int i=0; i < fields.length && i< 5; i++) {
			
			if(fieldNameStrings[i].equals("To")) {
				String[] recipients = fields[i].split(",");
				for(String recipient : recipients) {
					//recipient = recipients.trim();
					recipient = recipients.toString();

					// -- put field
					m.put(columnFamilies[i], new Text(recipient), new Value(recipient.getBytes()));

				}
			}
			else {
				//  put field
				m.put(columnFamilies[i], new Text(""), new Value(fields[i].getBytes()));
		
			}
		}
		// add mutation to writer
		writer.addMutation(m);
		line = reader.readLine();
	}
	// close writer
	writer.close();

	}
	
}
