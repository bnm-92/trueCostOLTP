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
	
	public void addSinglePartitionTransaction(String procedureName, int initiatorHostId, int partition, int latency)
	{
		m_groupStatsKey.reset(procedureName, initiatorHostId, partition);
		addTransaction(m_groupStatsKey, latency);
	}
	
	public void addMultiPartitionTransaction(String procedureName, int initiatorHostId, int latency)
	{
		m_groupStatsKey.reset(procedureName, initiatorHostId);
		addTransaction(m_groupStatsKey, latency);
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
	public void addTransaction(TxnGroupStatsKey groupStatsKey, int latency) {
		ArrayList<TxnGroupStats> groupStatsList = null;
		TxnGroupStats groupStats = null;

		if ((groupStatsList = m_stats.get(groupStatsKey)) != null) {
			groupStats = groupStatsList.get(groupStatsList.size() - 1);
		} else {
			groupStatsList = new ArrayList<TxnGroupStats>();
			m_stats.put(groupStatsKey, groupStatsList);
		}

		if (groupStats != null && groupStats.getNumTransactions() < MAX_TRANSACTIONS_PER_GROUP) {
			groupStats.recordTransactionLatency(latency);
		} else {
			groupStats = new TxnGroupStats(groupStatsKey);
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
