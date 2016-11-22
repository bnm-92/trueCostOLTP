package org.voltdb.repartitioner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

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
	private StatsList m_latencies = new StatsList();

	/**
	 * Cache the median latency calculated from all latencies in the group.
	 */
	private int m_medianLatency = -1;

	/**
	 * Record total network latencies for communication with each
	 * partition if its site is remote.
	 * 
	 */
	private Map<Integer, StatsList> m_remoteSiteNetworkLatencies;

	public TxnGroupStats(TxnGroupStatsKey key) {
		m_key = key;
	}

	public void recordTransactionLatency(int latency) {
		m_numTransactions++;
		m_latencies.add(latency);
	}
	
	public void recordRemoteSiteNetworkLatency(int siteId, int latency)
	{
		StatsList latencies = m_remoteSiteNetworkLatencies.get(siteId);
		
		if(latencies != null)
		{
			latencies.add(latency);
		}
		else
		{
			latencies = new StatsList();
			latencies.add(latency);
			m_remoteSiteNetworkLatencies.put(siteId, latencies);
		}
	}
	
	public int getMedianLatency() {
		return m_latencies.getMedian();
	}
	
	public int getMedianRemoteSiteNetworkLatency(int siteId)
	{
		StatsList latencies = m_remoteSiteNetworkLatencies.get(siteId);
		
		if(latencies != null)
		{
			return latencies.getMedian();
		}
		
		return 0;
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

	public int getPartition() {
		return m_key.getPartition();
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
