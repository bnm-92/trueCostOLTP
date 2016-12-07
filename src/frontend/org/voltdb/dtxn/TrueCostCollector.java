package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltdb.VoltDB;
import org.voltdb.catalog.Site;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.SiteMailbox;
import org.voltdb.messaging.StopAndCopyDoneMessage;
import org.voltdb.messaging.TrueCostTransactionStatsMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.repartitioner.PartitioningGenerator;
import org.voltdb.repartitioner.PartitioningGeneratorResult;
import org.voltdb.repartitioner.StatsList;
import org.voltdb.repartitioner.TxnGroupStatsKey;
import org.voltdb.repartitioner.WorkloadSampleStats;

public class TrueCostCollector extends Thread {

	private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

	private static final int EPOCH_LENGTH_MS = 10000;

	private static final int MAX_PARTITIONS_PER_HOST = 5;

	SiteMailbox m_mailbox = null;
	int hostId = -1;
	int siteId = -1;

	private static class TxnGroupLatencyStats {
		private StatsList m_localLatencies = new StatsList();
		private StatsList m_remoteLatencies = new StatsList();

		public void addLocalLatency(long latency) {
			m_localLatencies.add(latency);
		}

		public void addRemoteLatency(long latency) {
			m_remoteLatencies.add(latency);
		}

		public long getLocalLatency() {
			Long median = m_localLatencies.getMedian();

			return median != null ? median : 0;
		}

		public long getRemoteLatency() {
			Long median = m_remoteLatencies.getMedian();

			return median != null ? median : 0;
		}
	}

	public int[] allHostIds;
	public int[] allPartitionIds;
	TxnGroupStatsKey txnGroupStatsKey = new TxnGroupStatsKey("", 0, 0);
	ArrayList<TrueCostTransactionStats> receivedTxnStats = new ArrayList<TrueCostTransactionStats>();
	Map<TxnGroupStatsKey, TxnGroupLatencyStats> txnGroupLatencyStatsMap = new HashMap<TxnGroupStatsKey, TxnGroupLatencyStats>();
	public WorkloadSampleStats workloadSampleStats = new WorkloadSampleStats();
	public PartitioningGenerator partitioningGenerator;
	PartitioningGeneratorResult optimizedPartitioning;
	StopAndCopyRun stopAndCopyRun;

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
		allPartitionIds = new int[siteIds.size() - hostIds.size()];
		
		for (Integer siteId : siteIds) {
			Site site = st.getSiteForId(siteId);
			if (site.getIsexec()) {
				int partition = st.getPartitionForSite(siteId);
				consoleLog.info("site being given for partitioning is" + siteId + " with partition: " + partition);
				allPartitionIds[i] = partition;
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

	private String toString(Map<Integer, ArrayList<Integer>> map) {
		StringBuilder builder = new StringBuilder();

		if (map != null && map.size() > 0) {
			int i = 0;
			for (Entry<Integer, ArrayList<Integer>> entry : map.entrySet()) {
				if (i > 0) {
					builder.append("\n");
				}

				builder.append(entry.getKey());
				builder.append(":{");

				int j = 0;
				for (Integer n : entry.getValue()) {
					if (j > 0) {
						builder.append(",");
					}
					builder.append(n);
					++j;
				}

				builder.append("}");
				++i;
			}
		} else {
			return "(empty)";
		}

		return builder.toString();
	}

	private boolean isLocal(int siteId, int hostId) {
		SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
		if (st.m_hostsToSites.get(hostId).contains(siteId)) {
			return true;
		}
		return false;
	}
	
	HashMap<Integer, ArrayList<Integer>> getCurrentMapping() {
		HashMap <Integer, ArrayList<Integer>> hm = new HashMap <Integer, ArrayList<Integer>>();
		SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
		Set<Integer> hosts = st.getAllLiveHosts();
		for (Integer host : hosts) {
			ArrayList<Integer> sites = st.getAllSitesForHost(host);
			ArrayList<Integer> partitionsForMap = new ArrayList<Integer>();
			for (Integer site : sites) {
				Site curSite = st.getSiteForId(site);
				if (curSite.getIsexec()) {
					partitionsForMap.add(st.getPartitionForSite(site));
				}
			}
			hm.put(host, partitionsForMap);
		}
		return hm;
	}
	
	public void run() {
		while (!VoltDB.instance().isServerInitialized()) {
			consoleLog.info("Transaction statistics collector waiting for VoltDB to start");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// Swallow
			}
		}
		
//      System.out.println("auto fail");
        
      	SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
          Site[] sites = st.getAllSites();
          boolean toggle = false;
          for (int i=0; i<sites.length; i++) {
//          	System.out.println(i);
          	if (sites[i].getIsexec()) {
//          		System.out.println(Integer.parseInt(sites[i].getTypeName()));
          		if (toggle) {
          			toggle = false;
          			VoltDB.instance().getFaultDistributor().reportFault
                  	(new NodeFailureFault(VoltDB.instance().getCatalogContext().
                  			siteTracker.getHostForSite(Integer.parseInt(sites[i].getTypeName()) ),
                  			Integer.parseInt(sites[i].getTypeName()) ,true));
          			
          			try {
						Thread.sleep(2000L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
          			
          		} else {
          			toggle = true;
          		}
          	}
          }
      

		initializePartitioningGenerator();

		boolean isStoppingAndCopying = false;
		int outstandingStopAndCopyMsgs = 0;
		long epochEnd = System.currentTimeMillis() + EPOCH_LENGTH_MS;

		while (true) {
			VoltMessage message = m_mailbox.recvBlocking(Math.max(0, System.currentTimeMillis() - epochEnd));
			long now = System.currentTimeMillis();

			if (message != null) {
				if (!isStoppingAndCopying) {
					if (message instanceof TrueCostTransactionStatsMessage) {
						TrueCostTransactionStats[] txnStatsList = ((TrueCostTransactionStatsMessage) message)
								.getTxnStatsList();

						for (TrueCostTransactionStats txnStats : txnStatsList) {
							long latency = Math.max(1, txnStats.getLatency());
							int coordinatorSiteId = txnStats.getCoordinatorSiteId();
							int initiatorHostId = txnStats.getInitiatorHostId();

							receivedTxnStats.add(txnStats);

							if (txnStats.isSinglePartition()) {
								// Group together single-partition transactions
								// executing stored procedure at initiator
								txnGroupStatsKey.reset(txnStats.getProcedureName(), txnStats.getInitiatorHostId(), 0);
							} else {
								// Group together multi-partition transactions
								// executing at initiator
								txnGroupStatsKey.reset(txnStats.getProcedureName(), txnStats.getInitiatorHostId());
							}

							TxnGroupLatencyStats txnGroupLatencyStats = txnGroupLatencyStatsMap.get(txnGroupStatsKey);
							if (txnGroupLatencyStats == null) {
								txnGroupLatencyStats = new TxnGroupLatencyStats();
								txnGroupLatencyStatsMap.put(txnGroupStatsKey.clone(), txnGroupLatencyStats);
							}

							if (txnStats.isSinglePartition()) {
								if (isLocal(coordinatorSiteId, initiatorHostId)) {
									// Coordinator and execution are the same,
									// so
									// recorded latency is local latency
									txnGroupLatencyStats.addLocalLatency(latency);
								} else {
									// Execution site not on local host, record
									// as a
									// remote latency
									txnGroupLatencyStats.addRemoteLatency(latency);
								}
							} else {
								// Estimate the local latency to be 10% of the
								// recorded latency
								long localLatency = Math.max(1, Math.round((double) latency * 0.10));

								txnGroupLatencyStats.addLocalLatency(localLatency);
								txnGroupLatencyStats.addRemoteLatency(latency - localLatency);
							}
						}
					}
				} else if (outstandingStopAndCopyMsgs > 0) {
					if(message instanceof StopAndCopyDoneMessage) {
						--outstandingStopAndCopyMsgs;
						System.out.println("received message, left: " + outstandingStopAndCopyMsgs);
						isStoppingAndCopying = outstandingStopAndCopyMsgs > 0;
						
						if(!isStoppingAndCopying) {
							stopAndCopyRun.crashSource();
						}
					}
				}
			} else {
				consoleLog.warn("Received a null message");
			}

			if (now >= epochEnd) {
				if (receivedTxnStats.size() > 0) {
					consoleLog.info("Received " + receivedTxnStats.size() + " transaction stats this epoch");

					for (Entry<TxnGroupStatsKey, TxnGroupLatencyStats> txnGroupLatencyStatsMapEntry : txnGroupLatencyStatsMap
							.entrySet()) {
						TxnGroupStatsKey txnGroupStatsKey = txnGroupLatencyStatsMapEntry.getKey();
						TxnGroupLatencyStats txnGroupLatencyStats = txnGroupLatencyStatsMapEntry.getValue();

						if (txnGroupLatencyStats.getLocalLatency() > 0 || txnGroupLatencyStats.getRemoteLatency() > 0) {
							if (txnGroupLatencyStats.getLocalLatency() == 0) {
								// Estimate the local latency to be 50% of the
								// remote latency
								long localLatency = Math.max(1,
										Math.round((double) txnGroupLatencyStats.getRemoteLatency() * 0.50));
								txnGroupLatencyStats.addLocalLatency(localLatency);
							} else if (txnGroupLatencyStats.getRemoteLatency() == 0) {
								// Estimate the remote latency to be 2x the
								// local latency
								txnGroupLatencyStats.addRemoteLatency(txnGroupLatencyStats.getLocalLatency() * 2);
							}

							consoleLog.info("Transaction group latency stats. Group: " + txnGroupStatsKey + ", Local: "
									+ txnGroupLatencyStats.getLocalLatency() + "ms, Remote: "
									+ txnGroupLatencyStats.getRemoteLatency() + "ms");
						} else {
							consoleLog.warn("Could not get local or remote latency for transaction group "
									+ txnGroupLatencyStatsMapEntry.getKey());
						}
					}

					for (TrueCostTransactionStats txnStats : receivedTxnStats) {
						TxnGroupLatencyStats txnGroupLatencyStats = null;
						long localLatency = 0L;
						long remoteLatency = 0L;

						if (txnStats.isSinglePartition()) {
							txnGroupStatsKey.reset(txnStats.getProcedureName(), txnStats.getInitiatorHostId(), 0);
							txnGroupLatencyStats = txnGroupLatencyStatsMap.get(txnGroupStatsKey);

							if (txnGroupLatencyStats != null) {
								localLatency = txnGroupLatencyStats.getLocalLatency();
								remoteLatency = txnGroupLatencyStats.getRemoteLatency();

								if (localLatency > 0 && remoteLatency > 0) {
									workloadSampleStats.addSinglePartitionTransaction(txnStats.getProcedureName(),
											txnStats.getInitiatorHostId(), txnStats.getPartition(),
											localLatency + remoteLatency);
									workloadSampleStats.recordSinglePartitionTransactionRemotePartitionNetworkLatency(
											txnStats.getProcedureName(), txnStats.getInitiatorHostId(),
											txnStats.getPartition(), remoteLatency, true);
								} else {
									consoleLog.warn(
											"Could not get local and remote latency stats for transaction " + txnStats);
								}
							} else {
								consoleLog.warn(
										"Could not get transaction group latency stats for transaction " + txnStats);
							}
						} else {
							txnGroupStatsKey.reset(txnStats.getProcedureName(), txnStats.getInitiatorHostId());
							txnGroupLatencyStats = txnGroupLatencyStatsMap.get(txnGroupStatsKey);

							if (txnGroupLatencyStats != null) {
								localLatency = txnGroupLatencyStats.getLocalLatency();
								remoteLatency = txnGroupLatencyStats.getRemoteLatency();

								if (localLatency > 0 && remoteLatency > 0) {
									workloadSampleStats.addMultiPartitionTransaction(txnStats.getProcedureName(),
											txnStats.getInitiatorHostId(), localLatency + remoteLatency);

									for (int partition : allPartitionIds) {
										workloadSampleStats
												.recordMultiPartitionTransactionRemotePartitionNetworkLatency(
														txnStats.getProcedureName(), txnStats.getInitiatorHostId(),
														partition, remoteLatency, true);
									}
								} else {
									consoleLog.warn(
											"Could not get local and remote latency stats for transaction " + txnStats);
								}
							} else {
								consoleLog.warn(
										"Could not get transaction group latency stats for transaction " + txnStats);
							}
						}
					}

					consoleLog.info("Generating optimum partitioning");
					initializePartitioningGenerator();
					optimizedPartitioning = partitioningGenerator.findOptimumPartitioning(workloadSampleStats);

					if (optimizedPartitioning != null) {
						consoleLog.info("Generated optimum partitioning:\n"
								+ toString(optimizedPartitioning.getHostToPartitionsMap()));
						consoleLog.info("Estimated execution time under optimum partitioning: "
								+ optimizedPartitioning.getEstimatedExecTime());
					} else {
						consoleLog.warn("Could not generate optimum partitioning!");
					}
					
					// TODO: Decide if we should repartition or not
					stopAndCopyRun = new StopAndCopyRun();
					outstandingStopAndCopyMsgs = stopAndCopyRun.doStopAndCopy(optimizedPartitioning.getHostToPartitionsMap());
					System.out.println("reinitializing stop and copy but waiting for " + outstandingStopAndCopyMsgs);
					isStoppingAndCopying = true;
				}

				receivedTxnStats.clear();
				txnGroupLatencyStatsMap.clear();
				workloadSampleStats.clearStats();

				epochEnd = System.currentTimeMillis() + EPOCH_LENGTH_MS;
			}
		}
	}
}
