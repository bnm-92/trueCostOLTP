package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltdb.VoltDB;
import org.voltdb.catalog.Site;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.SiteMailbox;
import org.voltdb.messaging.TrueCostTransactionStatsMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.repartitioner.PartitioningGenerator;
import org.voltdb.repartitioner.WorkloadSampleStats;

public class TrueCostCollector extends Thread {

	private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

	private static final int EPOCH_LENGTH_MS = 10000;

	private static final int MAX_PARTITIONS_PER_HOST = 5;

	SiteMailbox m_mailbox = null;
	int hostId = -1;
	int siteId = -1;

	class AggregatedTrueCostTransactionStats {
		public TrueCostTransactionStats txnStats;
		public long localLatency;
		public long remoteLatency;
	}

	class StatsGroupedByInitiator {
		public int inititatorId;
		public long avgLocalLatency;
		public long avgRemoteLatency;

		public long totalLocalLatency;
		public long totalRemoteLatency;
		public int totalLocalTxns;
		public int totalRemoteTxns;

		StatsGroupedByInitiator() {
			inititatorId = 0;
			avgLocalLatency = 0L;
			avgRemoteLatency = 0L;

			totalLocalLatency = 0L;
			totalRemoteLatency = 0L;
			totalLocalTxns = 0;
			totalRemoteTxns = 0;
		}
	}

	public int[] allHostIds;
	public int[] allPartitionIds;
	ArrayList<AggregatedTrueCostTransactionStats> arr = new ArrayList<AggregatedTrueCostTransactionStats>();
	public HashMap<Integer, StatsGroupedByInitiator> initiatorToStatsMap = new HashMap<Integer, StatsGroupedByInitiator>();
	public WorkloadSampleStats workloadSampleStats = new WorkloadSampleStats();
	public PartitioningGenerator partitioningGenerator;

	TrueCostCollector(int siteId, int hostId) {
		setDaemon(true);

		this.siteId = siteId;
		this.hostId = hostId;
		m_mailbox = (SiteMailbox) VoltDB.instance().getMessenger().createMailbox(siteId, VoltDB.COLLECTOR_MAILBOX_ID,
				false);
	}

	private void initializePartitioningGenerator() {
		int i = 0;

		SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
		Set<Integer> hostIds = st.m_liveHostIds;
		Set<Integer> siteIds = st.m_liveSiteIds;

		allHostIds = new int[hostIds.size()];
		allPartitionIds = new int[siteIds.size()-hostIds.size()];

		for (Integer siteId : siteIds) {
			consoleLog.info("site is : " + siteId);
			Site site = st.getSiteForId(siteId);
			if (site.getIsexec()) {
				allPartitionIds[i] = st.getPartitionForSite(siteId);
				++i;
			}
		}

		i = 0;
		for (Integer hostId : hostIds) {
			allHostIds[i] = hostId;
			++i;
		}

		partitioningGenerator = new PartitioningGenerator(allHostIds, allPartitionIds, MAX_PARTITIONS_PER_HOST);
	}

	private boolean isLocal(int siteId, int hostId) {
		SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
		if (st.m_hostsToSites.get(hostId).contains(siteId)) {
			return true;
		}
		return false;
	}

	public void run() {
		while(!VoltDB.instance().isServerInitialized()) {
			consoleLog.info("Transaction statistics collector waiting for VoltDB to start");
			try {
				Thread.sleep(1000);
			} catch(InterruptedException e) {
				// Swallow
			}
		}
		
		initializePartitioningGenerator();
		
		long epochEnd = System.currentTimeMillis() + EPOCH_LENGTH_MS;

		while (true) {
			VoltMessage message = m_mailbox.recvBlocking(Math.max(0, System.currentTimeMillis() - epochEnd));
			long now = System.currentTimeMillis();

			if (message != null) {
				if (message instanceof TrueCostTransactionStatsMessage) {
					TrueCostTransactionStats[] txnStatsList = ((TrueCostTransactionStatsMessage) message)
							.getTxnStatsList();

					for (TrueCostTransactionStats txnStats : txnStatsList) {
						AggregatedTrueCostTransactionStats aggTxnStats = new AggregatedTrueCostTransactionStats();
						aggTxnStats.txnStats = txnStats;
						arr.add(aggTxnStats);
					}
				}
			}

			if (now >= epochEnd) {
				if (arr.size() > 0) {

					for (AggregatedTrueCostTransactionStats aggTxnStats : arr) {
						TrueCostTransactionStats txnStats = aggTxnStats.txnStats;

						int siteId = txnStats.getCoordinatorSiteId();
						int hostId = txnStats.getInitiatorHostId();
						int initId = txnStats.getInitiatorSiteId();

						StatsGroupedByInitiator sgbi = null;
						if (this.initiatorToStatsMap.containsKey(initId)) {
							sgbi = initiatorToStatsMap.get(initId);
						} else {
							sgbi = new StatsGroupedByInitiator();
							this.initiatorToStatsMap.put(initId, sgbi);
						}

						if (isLocal(siteId, hostId)) {
							sgbi.totalLocalLatency += txnStats.getLatency();
							sgbi.totalLocalTxns++;
						} else {
							sgbi.totalRemoteLatency += txnStats.getLatency();
							sgbi.totalRemoteTxns++;
						}
					}

					for (Entry<Integer, StatsGroupedByInitiator> initiatorToStatsMapEntry : initiatorToStatsMap
							.entrySet()) {
						StatsGroupedByInitiator sgbi = initiatorToStatsMapEntry.getValue();
						if (sgbi.totalLocalTxns > 0) {
							sgbi.avgLocalLatency = (sgbi.totalLocalLatency / sgbi.totalLocalTxns);
						}
						if (sgbi.totalRemoteTxns > 0) {
							sgbi.avgRemoteLatency = (sgbi.totalRemoteLatency / sgbi.totalRemoteTxns);
						}
					}

					for (AggregatedTrueCostTransactionStats aggTxnStats : arr) {
						TrueCostTransactionStats txnStats = aggTxnStats.txnStats;

						int siteId = txnStats.getCoordinatorSiteId();
						int hostId = txnStats.getInitiatorHostId();
						int initId = txnStats.getInitiatorSiteId();

						StatsGroupedByInitiator sgbi = initiatorToStatsMap.get(initId);
						if (isLocal(siteId, hostId)) {
							aggTxnStats.localLatency = txnStats.getLatency();
							aggTxnStats.remoteLatency = sgbi.avgRemoteLatency;
						} else {
							aggTxnStats.localLatency = sgbi.avgLocalLatency;
							aggTxnStats.remoteLatency = txnStats.getLatency();
						}

						if (txnStats.isSinglePartition()) {
							workloadSampleStats.addSinglePartitionTransaction(txnStats.getProcedureName(),
									txnStats.getInitiatorHostId(), txnStats.getPartition(),
									aggTxnStats.localLatency + aggTxnStats.remoteLatency);
							workloadSampleStats.recordSinglePartitionTransactionRemotePartitionNetworkLatency(
									txnStats.getProcedureName(), txnStats.getInitiatorHostId(), txnStats.getPartition(),
									aggTxnStats.remoteLatency, false);
						} else {
							workloadSampleStats.addMultiPartitionTransaction(txnStats.getProcedureName(),
									txnStats.getInitiatorHostId(),
									aggTxnStats.localLatency + aggTxnStats.remoteLatency);

							for (int partition : allPartitionIds) {
								workloadSampleStats.recordMultiPartitionTransactionRemotePartitionNetworkLatency(
										txnStats.getProcedureName(), txnStats.getInitiatorHostId(), partition,
										aggTxnStats.remoteLatency, false);
							}
						}

						consoleLog.info("Transaction statistics collector generating partitioning");
							Map<Integer, ArrayList<Integer>> hostToPartitionsMap = partitioningGenerator
									.findOptimumPartitioning(workloadSampleStats);
						consoleLog.info("Transaction statistics collector done generating partitioning");
					}

				}

				arr.clear();
				initiatorToStatsMap.clear();
				workloadSampleStats.clearStats();

				epochEnd = System.currentTimeMillis() + EPOCH_LENGTH_MS;
			}
		}
	}
}
