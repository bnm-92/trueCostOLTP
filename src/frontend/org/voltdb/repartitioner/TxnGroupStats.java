package org.voltdb.repartitioner;

import java.util.ArrayList;
import java.util.Collections;

/**
 * <p>
 * Aggregated statistics for a group of transactions which share the same:
 * </p>
 * <ul>
 * <li>Stored Procedure</li>
 * <li>Initiator</li>
 * <li>Partitions Used</li>
 * </ul>
 */
public class TxnGroupStats implements Comparable<TxnGroupStats> {
	/**
	 * Key for this group (stored procedure, initiator, partitions used).
	 */
	private TxnGroupStatsKey m_key;

	/**
	 * Number of transactions in the group.
	 */
	private int m_numTransactions;

	/**
	 * Latencies for transactions in the group.
	 */
	private ArrayList<Integer> m_latencies = new ArrayList<Integer>();

	/**
	 * Cache the median latency calculated from all latencies in the group.
	 */
	private int m_medianLatency = -1;

	/**
	 * TODO: Record total network latencies for communication with each
	 * partition if its site is remote.
	 * 
	 */

	public TxnGroupStats(TxnGroupStatsKey key) {
		m_key = key;
	}

	public void recordTransactionLatency(int latency) {
		m_numTransactions++;
		m_latencies.add(latency);
	}

	public int getMedianLatency() {
		// Group should not exist with no transactions.
		assert (m_numTransactions > 0);

		if (m_medianLatency == -1) {
			if (m_numTransactions > 1) {
				Collections.sort(m_latencies);

				if (m_numTransactions % 2 != 0) {
					m_medianLatency = m_latencies.get(m_numTransactions / 2);
				} else {
					m_medianLatency = Math.round((float) (m_latencies.get(m_numTransactions / 2 - 1) + m_latencies.get(m_numTransactions / 2)) / 2);
				}
			} else {
				m_medianLatency = m_latencies.get(0);
			}
		}

		return m_medianLatency;
	}
	
	public boolean isSinglePartition()
	{
		return m_key.isSinglePartition();
	}

	public String getProcedureName() {
		return m_key.getProcedureName();
	}

	public int getInitiatorHostId() {
		return m_key.getInitiatorHostId();
	}

	public int[] getPartitions() {
		return m_key.getPartitions();
	}
	
	public int getNumTransactions() {
		return m_numTransactions;
	}

	@Override
	public int compareTo(TxnGroupStats o) {
		if(getMedianLatency() < o.getMedianLatency())
		{
			return -1;
		}
		else if(getMedianLatency() == o.getMedianLatency())
		{
			return 0;
		}
		else
		{
			return 1;
		}
	}
}
