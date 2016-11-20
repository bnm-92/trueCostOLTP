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

	private Map<TxnGroupStatsKey, ArrayList<TxnGroupStats>> m_stats = new HashMap<TxnGroupStatsKey, ArrayList<TxnGroupStats>>();

	private ArrayList<TxnGroupStats> m_singlePartitionTxnStats = new ArrayList<TxnGroupStats>();

	private ArrayList<TxnGroupStats> m_multiPartitionTxnStats = new ArrayList<TxnGroupStats>();
	
	public ArrayList<TxnGroupStats> getSinglePartitionTxnStats()
	{
		return m_singlePartitionTxnStats;
	}
	
	public ArrayList<TxnGroupStats> getMultiPartitionTxnStats()
	{
		return m_multiPartitionTxnStats;
	}

	/**
	 * Add a transaction and its latency statistics to the workload sample
	 * statistics.
	 * 
	 * @param procedureName
	 *            transaction stored procedure name.
	 * @param initiatorHostId
	 *            host id of the initiator for the transaction when it ran.
	 * @param partitions
	 *            partitions used by the transaction.
	 * @param latency
	 *            transaction execution latency.
	 */
	public void addTransaction(String procedureName, int initiatorHostId, int[] partitions, int latency) {
		// TODO: Maybe optimize away the creation of this key each time
		TxnGroupStatsKey groupStatsKey = new TxnGroupStatsKey(procedureName, initiatorHostId, partitions);
		ArrayList<TxnGroupStats> groupStatsList = null;
		TxnGroupStats groupStats = null;

		if ((groupStatsList = m_stats.get(groupStatsKey)) != null) {
			groupStats = groupStatsList.get(groupStatsList.size()-1);
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
}
