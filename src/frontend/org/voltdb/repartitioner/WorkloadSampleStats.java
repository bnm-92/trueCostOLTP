package org.voltdb.repartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 *
 */
public class WorkloadSampleStats {
	private static final int MAX_TRANSACTIONS_PER_GROUP = 1000;

	private int m_maxTransactionsPerGroup = MAX_TRANSACTIONS_PER_GROUP;
	
	private TxnGroupStatsKey m_groupStatsKey = new TxnGroupStatsKey("", 0, 0);

	private Map<TxnGroupStatsKey, ArrayList<TxnGroupStats>> m_stats = new HashMap<TxnGroupStatsKey, ArrayList<TxnGroupStats>>();

	private ArrayList<TxnGroupStats> m_singlePartitionTxnStats = new ArrayList<TxnGroupStats>();

	private ArrayList<TxnGroupStats> m_multiPartitionTxnStats = new ArrayList<TxnGroupStats>();

	public ArrayList<TxnGroupStats> getSinglePartitionTxnStats() {
		return m_singlePartitionTxnStats;
	}

	public ArrayList<TxnGroupStats> getMultiPartitionTxnStats() {
		return m_multiPartitionTxnStats;
	}
	
	public WorkloadSampleStats()
	{
	}
	
	public WorkloadSampleStats(int maxTransactionsPerGroup)
	{
		m_maxTransactionsPerGroup = maxTransactionsPerGroup;
	}

	/**
	 * Add a single-partition transaction to the workload sample statistics.
	 * 
	 * @param procedureName
	 * @param initiatorHostId
	 * @param partition
	 * @param latency
	 */
	public void addSinglePartitionTransaction(String procedureName, int initiatorHostId, int partition, int latency) {
		m_groupStatsKey.reset(procedureName, initiatorHostId, partition);
		addTransaction(m_groupStatsKey, latency);
	}

	/**
	 * Add a multi-(every)-partition transaction to the workload sample
	 * statistics.
	 * 
	 * @param procedureName
	 * @param initiatorHostId
	 * @param latency
	 */
	public void addMultiPartitionTransaction(String procedureName, int initiatorHostId, int latency) {
		m_groupStatsKey.reset(procedureName, initiatorHostId);
		addTransaction(m_groupStatsKey, latency);
	}

	/**
	 * Record the total network latency of communication with the given site
	 * (assuming it is remote) for a single-partition transaction to the
	 * workload sample statistics.
	 * 
	 * @param procedureName
	 * @param initiatorHostId
	 * @param partition
	 * @param siteId
	 * @param latency
	 * @param isEstimate
	 */
	public void recordSinglePartitionTransactionRemotePartitionNetworkLatency(String procedureName, int initiatorHostId, int partition, 
			int latency, boolean isEstimate) {
		m_groupStatsKey.reset(procedureName, initiatorHostId, partition);
		recordRemotePartitionNetworkLatency(m_groupStatsKey, partition, latency, isEstimate);
	}

	/**
	 * Record the total network latency of communication with the given site
	 * (assuming it is remote) for a multi-(every)-partition transaction to the
	 * workload sample statistics.
	 * 
	 * @param procedureName
	 * @param initiatorHostId
	 * @param partition
	 * @param siteId
	 * @param latency
	 * @param isEstimate
	 */
	public void recordMultiPartitionTransactionRemotePartitionNetworkLatency(String procedureName, int initiatorHostId, int partition, int latency, boolean isEstimate) {
		m_groupStatsKey.reset(procedureName, initiatorHostId);
		recordRemotePartitionNetworkLatency(m_groupStatsKey, partition, latency, isEstimate);
	}

	private void recordRemotePartitionNetworkLatency(TxnGroupStatsKey groupStatsKey, int partition, int latency, boolean isEstimate) {
		ArrayList<TxnGroupStats> groupStatsList = m_stats.get(groupStatsKey);

		assert (groupStatsList != null);
		assert (!groupStatsList.isEmpty());

		groupStatsList.get(groupStatsList.size() - 1).recordRemotePartitionNetworkLatency(partition, latency, isEstimate);
	}

	/**
	 * Add a transaction and its latency statistics to the workload sample
	 * statistics.
	 * 
	 * @param groupStatsKey
	 *            key for the transaction group this transaction's stats should
	 *            be added to
	 * @param latency
	 *            transaction execution latency.
	 */
	private void addTransaction(TxnGroupStatsKey groupStatsKey, int latency) {
		ArrayList<TxnGroupStats> groupStatsList = null;
		TxnGroupStats groupStats = null;

		if ((groupStatsList = m_stats.get(groupStatsKey)) != null) {
			groupStats = groupStatsList.get(groupStatsList.size() - 1);
		} else {
			groupStatsList = new ArrayList<TxnGroupStats>();
			m_stats.put(groupStatsKey, groupStatsList);
		}

		if (groupStats != null && groupStats.getNumTransactions() < m_maxTransactionsPerGroup) {
			groupStats.recordTransactionLatency(latency);
		} else {
			groupStats = new TxnGroupStats(groupStatsKey.clone());
			groupStats.recordTransactionLatency(latency);
			groupStatsList.add(groupStats);

			if (groupStatsKey.isSinglePartition()) {
				m_singlePartitionTxnStats.add(groupStats);
			} else {
				m_multiPartitionTxnStats.add(groupStats);
			}
		}
	}

	public void clearStats() {
		m_stats.clear();
		m_singlePartitionTxnStats.clear();
		m_multiPartitionTxnStats.clear();
	}
}
