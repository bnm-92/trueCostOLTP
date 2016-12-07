package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.fault.NodeFailureFault;

public class StopAndCopyRun{
	ArrayList<String> commands = null;
	SiteTracker st = null;
	
	public int doStopAndCopy(Map<Integer, ArrayList<Integer>> hm) {
		this.st = VoltDB.instance().getCatalogContext().siteTracker;
		ArrayList<String> commands = new ArrayList<String>();
		Iterator it = hm.entrySet().iterator();
		int count = 0;
		while (it.hasNext()) {
			Map.Entry<Integer, ArrayList<Integer>> m = (Entry<Integer, ArrayList<Integer>>) it.next();
			int hostId = m.getKey();
			ArrayList<Integer> partitions = m.getValue();
			for (Integer partition : partitions) {
				if (!isLocal(hostId, partition)) {
					// add stop and copy command
					String destHost = Integer.toString(hostId);
					String destSite = (getDeadSiteForPartition(partition));
					String srcHost = (getAliveHostForPartition(partition));
					String srcSite = (getAliveSiteForPartition(partition));
					System.out.println("starting stop and copy for : " + 
							srcSite + " " + srcHost + " " + destSite + " " + destHost);
					stopAndCopy(srcSite, srcHost, destSite, destHost);
					System.out.println("stop and copy done for : " + 
							srcSite + " " + srcHost + " " + destSite + " " + destHost);
					commands.add(srcSite);
					count++;
					// add failing code later
				}
			}
		}
		System.out.println("returning " + count);
		return count;
	}
	
	boolean isLocal(int hostId, int partition) {
//		System.out.println("hostid is : " + hostId);
//		System.out.println("partition is : " + partition);
		ArrayList<Integer> sites = st.m_hostsToSites.get(hostId);
		for (int s: sites) {
			Site site = st.getSiteForId(s);
//			System.out.println("site was : " + s);
//			if (site == null)
			if (site.getIsup() && site.getIsexec())
				if (Integer.parseInt(site.getPartition().getTypeName()) == partition) {
				return true;
			}
		}
		return false;
	}
	
	String getDeadSiteForPartition(int p) {
		ArrayList<Integer> sites = st.m_partitionsToSites.get(p);
		for(int s : sites) {
			Site site = st.getSiteForId(s);
			if (!site.getIsup()) {
				return (Integer.toString(s));
			} 
		}
		return null;
	}
	String getAliveSiteForPartition(int p) {
		ArrayList<Integer> sites = st.m_partitionsToSites.get(p);
		for(int s : sites) {
			Site site = st.getSiteForId(s);
			if (site.getIsup()) {
				return (Integer.toString(s));
			} 
		}
		return null;
	}
	
	String getAliveHostForPartition(int p) {
		int siteId = Integer.parseInt(getAliveSiteForPartition(p));
		int hostId = st.getHostForSite(siteId);
		return Integer.toString(hostId);
	}
	
	void crashSource() {
		System.out.println("size of commands to crash: " + commands.size() + " " + commands.toString());
		for (String srcSite : commands) {
			int sourceSite = Integer.parseInt(srcSite);
			VoltDB.instance().getFaultDistributor().reportFault
			(new NodeFailureFault(VoltDB.instance().getCatalogContext().
				siteTracker.getHostForSite(sourceSite), sourceSite,true));
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void stopAndCopy(String srcSite, String srcHost, String destSite, String destHost) {
        try {
        	Client client = ClientFactory.createClient();
    		client.createConnection("localhost");
    		Object[] params = { srcHost,srcSite, destHost, destSite };
    		ClientResponse rsp = client.callProcedure("@StopAndCopy", params);
    		VoltTable[] vt = rsp.getResults();
    		client.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
		
	}
}
