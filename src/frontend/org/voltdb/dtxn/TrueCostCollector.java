package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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
import org.voltdb.repartitioner.TxnScheduleGraph;
import org.voltdb.repartitioner.WorkloadSampleStats;

public class TrueCostCollector extends Thread {

	private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

	/**
	 * Decide at the end of each epoch whether to repartition. Length of the
	 * epoch.
	 */
	private static final int EPOCH_LENGTH_MS = 1000;

	/**
	 * Maximum number of partitions per host. TODO: Retrieve this via
	 * configuration.
	 */
	private static final int MAX_PARTITIONS_PER_HOST = 5;

	/**
	 * At the end of each epoch we determine the estimated execution time of the
	 * sampled transactions on the current partition. Used to determine whether
	 * to repartition. Store the last N estimated execution times.
	 */
	private static final int LOOK_BACK_EPOCHS = 30;

	/**
	 * Minimum improvement percentage in estimated execution time on the optimum
	 * partitioning vs. the current partitioning in order to decide to
	 * repartition.
	 */
	private static final double MIN_REPARTITIONING_GAIN_PCT = 0.20f;

	/**
	 * If this number of epochs have passed without a repartitioning, then we
	 * will repartition if there is any gain.
	 */
	private static final int THRESHOLD_EPOCHS_WITHOUT_REPARTITION = 60;

	/**
	 * Number of epochs to ignore after the collector has just started up.
	 */
	private static final int IGNORE_EPOCHS_AFTER_STARTUP = 20;

	/**
	 * Number of epochs after a repartitioning has just occurred to ignore.
	 */
	private static final int IGNORE_EPOCHS_AFTER_REPARTITION = 40;

	private static class TxnGroupLatencyStats {
		private StatsList m_localLatencies = new StatsList();
		private StatsList m_remoteLatencies = new StatsList();

		public void addLocalLatency(long latency) {
			m_localLatencies.add(latency);
		}

		public void setLocalLatency(long latency) {
			m_localLatencies.clear();
			m_localLatencies.add(latency);
		}

		public void addRemoteLatency(long latency) {
			m_remoteLatencies.add(latency);
		}

		public void setRemoteLatency(long latency) {
			m_remoteLatencies.clear();
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

	private SiteMailbox m_mailbox = null;
	private int[] m_allHostIds;
	private int[] m_allPartitionIds;
	private TxnGroupStatsKey m_txnGroupStatsKey = new TxnGroupStatsKey("", 0, 0);
	private ArrayList<TrueCostTransactionStats> m_receivedTxnStats = new ArrayList<TrueCostTransactionStats>();
	private Map<TxnGroupStatsKey, TxnGroupLatencyStats> m_txnGroupLatencyStatsMap = new HashMap<TxnGroupStatsKey, TxnGroupLatencyStats>();
	private WorkloadSampleStats m_workloadSampleStats = new WorkloadSampleStats();
	private PartitioningGenerator m_partitioningGenerator;
	private PartitioningGeneratorResult m_optimizedPartitioning;
	private StopAndCopyRun m_stopAndCopyRun;
	private LinkedList<Long> m_lastEstimatedExecTimes = new LinkedList<Long>();
	private int m_numEpochsWithoutRepartition;
	private int m_ignoreEpochs;

	TrueCostCollector() {
		setDaemon(true);

		m_mailbox = (SiteMailbox) VoltDB.instance().getMessenger().createMailbox(0, VoltDB.COLLECTOR_MAILBOX_ID, false);
	}

	private long getEstimatedExecTimesMean() {
		long total = 0L;
		int n = m_lastEstimatedExecTimes.size();

		if (n > 0) {
			for (Long execTime : m_lastEstimatedExecTimes) {
				total += execTime;
			}

			return Math.round((double) total / n);
		}

		return 0L;
	}

	private long getEstimatedExecTimesStandardDeviation() {
		long total = 0L;
		int n = m_lastEstimatedExecTimes.size();

		if (n > 2) {
			long meanExecTime = getEstimatedExecTimesMean();

			for (Long execTime : m_lastEstimatedExecTimes) {
				long factor = meanExecTime - execTime;
				total += factor * factor;
			}

			return Math.round(Math.sqrt((double) total / (n - 1)));
		}

		return 0L;
	}

	private void initializePartitioningGenerator() {
		int i = 0;

		SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
		Set<Integer> hostIds = st.m_liveHostIds;
		Set<Integer> siteIds = st.m_liveSiteIds;

		m_allHostIds = new int[hostIds.size()];
		m_allPartitionIds = new int[siteIds.size() - hostIds.size()];

		for (Integer siteId : siteIds) {
			Site site = st.getSiteForId(siteId);
			if (site.getIsexec()) {
				int partition = st.getPartitionForSite(siteId);
				// consoleLog.info("site being given for partitioning is" +
				// siteId + " with partition: " + partition);
				m_allPartitionIds[i] = partition;
				++i;
			}
		}

		i = 0;
		for (Integer hostId : hostIds) {
			m_allHostIds[i] = hostId;
			++i;
		}

		m_partitioningGenerator = new PartitioningGenerator(m_allHostIds, m_allPartitionIds, MAX_PARTITIONS_PER_HOST);
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

	HashMap<Integer, ArrayList<Integer>> getCurrentHostToPartititionsMap() {
		HashMap<Integer, ArrayList<Integer>> hm = new HashMap<Integer, ArrayList<Integer>>();
		SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
		Set<Integer> hosts = st.getAllLiveHosts();
		for (Integer host : hosts) {
			ArrayList<Integer> sites = st.getAllSitesForHost(host);
			ArrayList<Integer> partitionsForMap = new ArrayList<Integer>();
			for (Integer site : sites) {
				Site curSite = st.getSiteForId(site);
				if (curSite.getIsexec() && curSite.getIsup()) {
					partitionsForMap.add(st.getPartitionForSite(site));
				}
			}
			hm.put(host, partitionsForMap);
		}
		return hm;
	}

	private void initializeCrashedSites() {
		SiteTracker st = VoltDB.instance().getCatalogContext().siteTracker;
		Site[] sites = st.getAllSites();
		boolean toggle = false;
		for (int i = 0; i < sites.length; i++) {
			// System.out.println(i);
			if (sites[i].getIsexec()) {
				// System.out.println(Integer.parseInt(sites[i].getTypeName()));
				if (toggle) {
					toggle = false;
					VoltDB.instance().getFaultDistributor()
							.reportFault(new NodeFailureFault(
									VoltDB.instance().getCatalogContext().siteTracker
											.getHostForSite(Integer.parseInt(sites[i].getTypeName())),
									Integer.parseInt(sites[i].getTypeName()), true));

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
	}

	private void populateWorkloadSampleStats() {
		for (Entry<TxnGroupStatsKey, TxnGroupLatencyStats> txnGroupLatencyStatsMapEntry : m_txnGroupLatencyStatsMap
				.entrySet()) {
			TxnGroupStatsKey txnGroupStatsKey = txnGroupLatencyStatsMapEntry.getKey();
			TxnGroupLatencyStats txnGroupLatencyStats = txnGroupLatencyStatsMapEntry.getValue();
			
			if (txnGroupLatencyStats.getLocalLatency() == 0 || txnGroupLatencyStats.getRemoteLatency() == 0) {
				// Either there were no local txns or no remote txns
				
				if (txnGroupLatencyStats.getLocalLatency() == 0) {
					// Estimate the local latency to be 50% of the
					// remote latency
					long localLatency = Math.max(1,
							Math.round((double) txnGroupLatencyStats.getRemoteLatency() * 0.50));
					txnGroupLatencyStats.setLocalLatency(localLatency);
				} else if (txnGroupLatencyStats.getRemoteLatency() == 0) {
					// Estimate the remote latency to be 2x the
					// local latency
					txnGroupLatencyStats.setRemoteLatency(txnGroupLatencyStats.getLocalLatency() * 2);
				}
				
				if (txnGroupLatencyStats.getRemoteLatency() < txnGroupLatencyStats.getLocalLatency()) {
					// Indicates a logic error above
					throw new RuntimeException("Greater local latency (" + txnGroupLatencyStats.getLocalLatency()
							+ "ms) than remote latency (" + txnGroupLatencyStats.getRemoteLatency()
							+ "s) recorded for transaction group " + txnGroupLatencyStatsMapEntry.getKey());
				}
			} else if (txnGroupLatencyStats.getRemoteLatency() < txnGroupLatencyStats.getLocalLatency()) {
				consoleLog.warn("Greater local latency (" + txnGroupLatencyStats.getLocalLatency()
						+ "ms) than remote latency (" + txnGroupLatencyStats.getRemoteLatency()
						+ "s) recorded for transaction group " + txnGroupLatencyStatsMapEntry.getKey());

				// Indicates not a huge difference between local and remote
				// latency
				txnGroupLatencyStats.setRemoteLatency(txnGroupLatencyStats.getLocalLatency() * 2);
			}
			
			consoleLog.info("Transaction group latency stats. Group: " + txnGroupStatsKey + ", Local: "
					+ txnGroupLatencyStats.getLocalLatency() + "ms, Remote: "
					+ txnGroupLatencyStats.getRemoteLatency() + "ms");
		}

		for (TrueCostTransactionStats txnStats : m_receivedTxnStats) {
			TxnGroupLatencyStats txnGroupLatencyStats = null;
			long localLatency = 0L;
			long remoteLatency = 0L;

			if (txnStats.isSinglePartition()) {
				m_txnGroupStatsKey.reset(txnStats.getProcedureName(), txnStats.getInitiatorHostId(), 0);
				txnGroupLatencyStats = m_txnGroupLatencyStatsMap.get(m_txnGroupStatsKey);

				if (txnGroupLatencyStats != null) {
					localLatency = txnGroupLatencyStats.getLocalLatency();
					remoteLatency = txnGroupLatencyStats.getRemoteLatency();

					if (localLatency > 0 && remoteLatency > 0) {
						m_workloadSampleStats.addSinglePartitionTransaction(txnStats.getProcedureName(),
								txnStats.getInitiatorHostId(), txnStats.getPartition(), remoteLatency);
						m_workloadSampleStats.recordSinglePartitionTransactionRemotePartitionNetworkLatency(
								txnStats.getProcedureName(), txnStats.getInitiatorHostId(), txnStats.getPartition(),
								remoteLatency - localLatency, false);
					} else {
						consoleLog.warn("Could not get local and remote latency stats for transaction " + txnStats);
					}
				} else {
					consoleLog.warn("Could not get transaction group latency stats for transaction " + txnStats);
				}
			} else {
				m_txnGroupStatsKey.reset(txnStats.getProcedureName(), txnStats.getInitiatorHostId());
				txnGroupLatencyStats = m_txnGroupLatencyStatsMap.get(m_txnGroupStatsKey);

				if (txnGroupLatencyStats != null) {
					localLatency = txnGroupLatencyStats.getLocalLatency();
					remoteLatency = txnGroupLatencyStats.getRemoteLatency();

					if (localLatency > 0 && remoteLatency > 0) {
						m_workloadSampleStats.addMultiPartitionTransaction(txnStats.getProcedureName(),
								txnStats.getInitiatorHostId(), localLatency + remoteLatency);

						for (int partition : m_allPartitionIds) {
							m_workloadSampleStats.recordMultiPartitionTransactionRemotePartitionNetworkLatency(
									txnStats.getProcedureName(), txnStats.getInitiatorHostId(), partition,
									remoteLatency, false);
						}
					} else {
						consoleLog.warn("Could not get local and remote latency stats for transaction " + txnStats);
					}
				} else {
					consoleLog.warn("Could not get transaction group latency stats for transaction " + txnStats);
				}
			}
		}
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

		initializeCrashedSites();
		initializePartitioningGenerator();

		boolean isStoppingAndCopying = false;
		int outstandingStopAndCopyMsgs = 0;
		long epochEnd = System.currentTimeMillis() + EPOCH_LENGTH_MS;

		if (IGNORE_EPOCHS_AFTER_STARTUP > 0) {
			consoleLog.info("Transaction statistics collector will wait for "
					+ IGNORE_EPOCHS_AFTER_STARTUP * EPOCH_LENGTH_MS
					+ "ms before running repartitioning decision logic");

			m_ignoreEpochs = IGNORE_EPOCHS_AFTER_STARTUP;
		}

		while (true) {
			VoltMessage message = m_mailbox.recvBlocking(m_ignoreEpochs > 0 ? EPOCH_LENGTH_MS : Math.max(0, System.currentTimeMillis() - epochEnd));
			long now = System.currentTimeMillis();

			if (message != null) {
				if (!isStoppingAndCopying && m_ignoreEpochs == 0) {
					if (message instanceof TrueCostTransactionStatsMessage) {
						TrueCostTransactionStats[] txnStatsList = ((TrueCostTransactionStatsMessage) message)
								.getTxnStatsList();

						for (TrueCostTransactionStats txnStats : txnStatsList) {
							long latency = Math.max(1, txnStats.getLatency());
							int coordinatorSiteId = txnStats.getCoordinatorSiteId();
							int initiatorHostId = txnStats.getInitiatorHostId();

							m_receivedTxnStats.add(txnStats);

							if (txnStats.isSinglePartition()) {
								// Group together single-partition transactions
								// executing stored procedure at initiator
								m_txnGroupStatsKey.reset(txnStats.getProcedureName(), txnStats.getInitiatorHostId(), 0);
							} else {
								// Group together multi-partition transactions
								// executing at initiator
								m_txnGroupStatsKey.reset(txnStats.getProcedureName(), txnStats.getInitiatorHostId());
							}

							TxnGroupLatencyStats txnGroupLatencyStats = m_txnGroupLatencyStatsMap
									.get(m_txnGroupStatsKey);
							if (txnGroupLatencyStats == null) {
								txnGroupLatencyStats = new TxnGroupLatencyStats();
								m_txnGroupLatencyStatsMap.put(m_txnGroupStatsKey.clone(), txnGroupLatencyStats);
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
					if (message instanceof StopAndCopyDoneMessage) {
						consoleLog.info(
								"Received stop and copy done message. Messages left: " + outstandingStopAndCopyMsgs);

						--outstandingStopAndCopyMsgs;
						isStoppingAndCopying = outstandingStopAndCopyMsgs > 0;

						if (!isStoppingAndCopying) {
							m_stopAndCopyRun.crashSource();
						}
					}
				}
			} else {
				consoleLog.warn("Received a null message");
			}

			if (now >= epochEnd) {
				if (m_ignoreEpochs == 0) {
					if (m_receivedTxnStats.size() > 0) {
						consoleLog.info("Received " + m_receivedTxnStats.size() + " transaction stats this epoch");

						populateWorkloadSampleStats();

						// Find the estimated execution time of the workload
						// sample
						// under the current partitioning
						long estimatedExecTime = TxnScheduleGraph.getEstimatedExecutionTime(m_workloadSampleStats,
								getCurrentHostToPartititionsMap());

						consoleLog.info("Current partitioning:\n" + toString(getCurrentHostToPartititionsMap()));
						consoleLog.info("Estimated execution time under current partitioning: " + estimatedExecTime);

						if (m_lastEstimatedExecTimes.size() == LOOK_BACK_EPOCHS) {
							long estimatedExecTimesMean = getEstimatedExecTimesMean();
							long estimatedExecTimesStdDev = getEstimatedExecTimesStandardDeviation();
							boolean shouldRepartition = false;

							consoleLog.info("Mean of last-" + LOOK_BACK_EPOCHS + " estimated execution times: "
									+ estimatedExecTimesMean);
							consoleLog.info("Standard Deviation of last-" + LOOK_BACK_EPOCHS
									+ " estimated execution times: " + estimatedExecTimesStdDev);

							consoleLog.info("Generating optimum partitioning");
							initializePartitioningGenerator();
							m_optimizedPartitioning = m_partitioningGenerator
									.findOptimumPartitioning(getCurrentHostToPartititionsMap(), m_workloadSampleStats, 2);

							if (m_optimizedPartitioning != null) {
								consoleLog.info("Generated optimum partitioning:\n"
										+ toString(m_optimizedPartitioning.getHostToPartitionsMap()));
								consoleLog.info("Estimated execution time under optimum partitioning: "
										+ m_optimizedPartitioning.getEstimatedExecTime());

								if (estimatedExecTime > estimatedExecTimesMean + 2 * estimatedExecTimesStdDev) {
									consoleLog.info("Estimated execution time is worse than 95% of last-"
											+ LOOK_BACK_EPOCHS + " execution times");

									// Should repartition if the estimated
									// execution
									// time is worse than 95% of the last epochs
									// This is an outlier an will not be
									// remembered
									// in the last-n estimated exec. times
									shouldRepartition = true;
								} else {
									// Bump off the last remember execution time
									// and
									// remember this one
									m_lastEstimatedExecTimes.removeFirst();
									m_lastEstimatedExecTimes.addLast(estimatedExecTime);

									if (Math.round(m_optimizedPartitioning.getEstimatedExecTime()) <= Math
											.round((double) estimatedExecTime * (1 - MIN_REPARTITIONING_GAIN_PCT))) {
										consoleLog
												.info("Estimated execution time under optimum partitioning is at least "
														+ (MIN_REPARTITIONING_GAIN_PCT * 100)
														+ "% less than estimated execution time under current partitioning");

										// Should repartition if the estimated
										// gain
										// from
										// the optimized partitioning exceeds
										// a minimum percentage of the estimated
										// execution time on the current
										// partitioning.
										shouldRepartition = true;
									} else if (Math.round(m_optimizedPartitioning
											.getEstimatedExecTime()) < estimatedExecTime
											&& m_numEpochsWithoutRepartition >= THRESHOLD_EPOCHS_WITHOUT_REPARTITION) {
										consoleLog.info(m_numEpochsWithoutRepartition
												+ " have passed without a repartitioning and there is a potential gain from the optimum partitioning");

										shouldRepartition = true;
										m_numEpochsWithoutRepartition = 0;
									} else {
										m_numEpochsWithoutRepartition++;
									}
								}

								if (shouldRepartition) {
									consoleLog.info("Executing stop and copy repartitioning");

									m_stopAndCopyRun = new StopAndCopyRun();
									outstandingStopAndCopyMsgs = m_stopAndCopyRun
											.doStopAndCopy(m_optimizedPartitioning.getHostToPartitionsMap());
									isStoppingAndCopying = true;

									consoleLog.info("Repartitioning decision logic will not execute for another "
											+ IGNORE_EPOCHS_AFTER_REPARTITION * EPOCH_LENGTH_MS + "ms");
									m_ignoreEpochs = IGNORE_EPOCHS_AFTER_REPARTITION;
								}

							} else {
								consoleLog.warn("Could not generate optimum partitioning!");
							}

						} else {
							m_lastEstimatedExecTimes.add(estimatedExecTime);
						}
					}
				} else {
					consoleLog.info("Did not run repartitioning decision logic this epoch");

<<<<<<< Updated upstream
					// Epoch ignored
					m_ignoreEpochs = Math.max(0, m_ignoreEpochs - 1);
=======
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
//					stopAndCopyRun = new StopAndCopyRun();
//					outstandingStopAndCopyMsgs = stopAndCopyRun.doStopAndCopy(optimizedPartitioning.getHostToPartitionsMap());
//					System.out.println("reinitializing stop and copy but waiting for " + outstandingStopAndCopyMsgs);
//					isStoppingAndCopying = true;
>>>>>>> Stashed changes
				}

				m_receivedTxnStats.clear();
				m_txnGroupLatencyStatsMap.clear();
				m_workloadSampleStats.clearStats();

				epochEnd = System.currentTimeMillis() + EPOCH_LENGTH_MS;
			}
		}
	}
}
