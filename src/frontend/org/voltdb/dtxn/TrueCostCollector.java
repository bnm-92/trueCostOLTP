package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.voltdb.VoltDB;
import org.voltdb.messaging.SiteMailbox;
import org.voltdb.messaging.TrueCostTransactionStatsMessage;
import org.voltdb.messaging.VoltMessage;

public class TrueCostCollector extends Thread {
	int epoch = 0;
	int reset_epoch = 10;
	
	SiteMailbox m_mailbox = null;
	int hostId = -1;
	int siteId = -1;
	public volatile boolean m_shouldRun = false;
	
	 class AggrgatedTrueCostTransactionStats {
		 TrueCostTransactionStats ts;
		 Long local_latency;
		 Long remote_latency;
	 }
	 
	 ArrayList<AggrgatedTrueCostTransactionStats> arr = null;
	 
	 class StatsGroupedByInitiator {
		 int inititatorId;
		 Long avg_local_latency;
		 Long avg_remote_latency;
		 
		 Long total_local_latency;
		 Long total_remote_latency;
		 Long total_local;
		 Long total_remote;
		 
		 StatsGroupedByInitiator (){
			 inititatorId = 0;
			 avg_local_latency = (long) 0;
			 avg_remote_latency = (long) 0;
			 
			 total_local_latency = (long) 0;
			 total_remote_latency = (long) 0;
			 total_local = (long) 0;
			 total_remote = (long) 0;
		 }
	 }
	 
	 HashMap<Integer, StatsGroupedByInitiator> m_initiator_hashmap = null;
	 
	 private boolean isLocal(int siteId, int hostId) {
		 SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
		 if (st.m_hostsToSites.get(hostId).contains(siteId)) {
			 return true;
		 }
		 return false;
	 }
	 
	public void run() {
		while (m_shouldRun) {
			try {
				epoch++;
				VoltMessage message = m_mailbox.recvBlocking(500);
				if (message instanceof TrueCostTransactionStatsMessage ) {
					TrueCostTransactionStats[] tr_arr = null;// = message.getTxnStatsList();
					for (int i=0; i< tr_arr.length; i++) {
						int siteId = tr_arr[i].getCoordinatorSiteId();
						int hostId = tr_arr[i].getInitiatorHostId();
						int initId = tr_arr[i].getInitiatorSiteId();
						
						StatsGroupedByInitiator sgbi = null;
						if (this.m_initiator_hashmap.containsKey(initId)) {
							sgbi = m_initiator_hashmap.get(initId);
						}
						else {
							sgbi = new StatsGroupedByInitiator();
							this.m_initiator_hashmap.put(initId, sgbi);
						}
						
						if (isLocal(siteId, hostId)) {
							sgbi.total_local_latency += tr_arr[i].getLatency();
							sgbi.total_local++;
						} 
						else {
							sgbi.total_remote_latency += tr_arr[i].getLatency();
							sgbi.total_remote++;
						}
					}
					Iterator it = m_initiator_hashmap.entrySet().iterator();
					while ( it.hasNext()) {
						StatsGroupedByInitiator sgbi = (StatsGroupedByInitiator) it.next();
						sgbi.avg_local_latency = (sgbi.total_local_latency/sgbi.total_local);
						if (sgbi.avg_local_latency < 0) {
							sgbi.avg_local_latency = 0L;
							System.out.println("fix overflow bug");
						}
						
						sgbi.avg_remote_latency = (sgbi.total_remote_latency/sgbi.total_remote);
						if (sgbi.avg_remote_latency < 0) {
							sgbi.avg_remote_latency = 0L;
							System.out.println("fix overflow bug2");
						}
					}
					
					for (int i=0; i< tr_arr.length; i++) {
						int siteId = tr_arr[i].getCoordinatorSiteId();
						int hostId = tr_arr[i].getInitiatorHostId();
						int initId = tr_arr[i].getInitiatorSiteId();
						AggrgatedTrueCostTransactionStats atct = new AggrgatedTrueCostTransactionStats();
						atct.ts = tr_arr[i];
						StatsGroupedByInitiator sgbi = m_initiator_hashmap.get(initId);
						if (isLocal(siteId, hostId)) {
							atct.local_latency = tr_arr[i].getLatency();
							atct.remote_latency = sgbi.avg_remote_latency;
						} 
						else {
							atct.local_latency = sgbi.avg_local_latency;
							atct.remote_latency = tr_arr[i].getLatency();
						} 
					}
				} 
				else {
					
				}
				
				if (epoch >= reset_epoch) {
					epoch = 0;
					// run model, do repartitioning, stc and drain
				} 
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	TrueCostCollector (int siteId, int hostId) {
		this.m_shouldRun = true;
		this.siteId = siteId;
		this.hostId = hostId;
		arr = new ArrayList<AggrgatedTrueCostTransactionStats>(); 
		m_initiator_hashmap = new HashMap<Integer, StatsGroupedByInitiator>();
		m_mailbox = (SiteMailbox) VoltDB.instance().getMessenger().createMailbox(
                siteId,
                VoltDB.COLLECTOR_MAILBOX_ID,
                true);
	}
	
	public void shutDown() {
		this.m_shouldRun = false;
	}
	
}
