package voltkv;


import org.voltdb.CLIConfig;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;

import voltkv.AsyncBenchmark.StatusListener;

public class CustomClient {

    // Reference to the database connection we will use
    final Client client = null;


    
    public static void main(String[] args) throws Exception {
    	String input = args[0];
    	
        Client client = ClientFactory.createClient();
		
		if (input.equals("Fail")) {
			try{
				System.out.println("called FailSite SysProc");
				client.createConnection("localhost");
				Object[] params = {args[1], args[2]};
				ClientResponse rsp = client.callProcedure("@FailSite", params);
				VoltTable[] vt = rsp.getResults();

			}catch(Exception e) {
				
				VoltDB.crashLocalVoltDB("Problem connecting client to localHost" 
						+ e.getMessage(), false, e);
			
			}

		} else if (input.equals("STC")) {
//			assert(args.length >= 4);
			try{
				System.out.println("called STC");
				client.createConnection("localhost");
				System.out.println(args.length);
				for (int i=0; i<args.length; i++)
					System.out.println("at arg" + i + " is " + args[i]);
				Object[] params = {args[1], args[2], args[3], args[4]};
				ClientResponse rsp = client.callProcedure("@StopAndCopy", params);
				VoltTable[] vt = rsp.getResults();

			}catch(Exception e) {
				
				VoltDB.crashLocalVoltDB("Problem connecting client to localHost" 
						+ e.getMessage(), false, e);
			
			}

			
		}
    }
}
