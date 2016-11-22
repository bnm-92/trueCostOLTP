package org.voltdb.repartitioner;

import java.util.Map;
import java.util.Map.Entry;

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
	 * Record total network latencies for communication with each
	 * partition if its site is remote.
	 */
	private Map<Integer, StatsList> m_remoteSiteNetworkLatencies;
	
	/**
	 * Local latency of the transaction - latency if all sites are local.
	 */
	private int m_localLatency = -1;

	public TxnGroupStats(TxnGroupStatsKey key) {
		m_key = key;
	}

	public void recordTransactionLatency(int latency) {
		m_numTransactions++;
		m_latencies.add(latency);
	}
	
	public void recordRemoteSiteNetworkLatency(int siteId, int latency)
	{
		StatsList siteLatencies = m_remoteSiteNetworkLatencies.get(siteId);
		
		if(siteLatencies != null)
		{
			siteLatencies.add(latency);
		}
		else
		{
			siteLatencies = new StatsList();
			siteLatencies.add(latency);
			m_remoteSiteNetworkLatencies.put(siteId, siteLatencies);
		}
	}
	
	public int getMedianLatency() {
		return m_latencies.getMedian();
	}
	
	public int getMedianRemoteSiteNetworkLatency(int siteId)
	{
		StatsList siteLatencies = m_remoteSiteNetworkLatencies.get(siteId);
		
		if(siteLatencies != null)
		{
			return siteLatencies.getMedian();
		}
		
		return 0;
	}
	
	public int getLocalLatency()
	{
		if(m_localLatency < 0)
		{
			int maxRemoteSiteNetworkLatency = Integer.MIN_VALUE;
			
			for(Entry<Integer, StatsList> e : m_remoteSiteNetworkLatencies.entrySet())
			{
				StatsList siteLatencies = e.getValue();
				
				if(siteLatencies.getMedian() > maxRemoteSiteNetworkLatency)
				{
					maxRemoteSiteNetworkLatency = siteLatencies.getMedian();
				}
			}
			
			m_localLatency = Math.max(getMedianLatency() - maxRemoteSiteNetworkLatency, 0);
		}
		
		return m_localLatency;
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
