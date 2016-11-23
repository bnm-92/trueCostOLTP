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
	private Map<Integer, StatsList> m_remotePartitionNetworkLatencies;
	
	/**
	 * Local latency of the transaction - latency if all sites are local.
	 */
	private int m_localLatency = -1;

	public TxnGroupStats(TxnGroupStatsKey key) {
		m_key = key;
	}

	public void recordTransactionLatency(int latency) {
		assert(latency >= 0);
		
		m_numTransactions++;
		m_latencies.add(latency);
	}
	
	public void recordRemotePartitionNetworkLatency(int partition, int latency, boolean isEstimate)
	{
		assert(latency >= 0);
		
		StatsList latencies = m_remotePartitionNetworkLatencies.get(partition);
		
		if(latencies != null)
		{
			latencies.add(latency);
		}
		else
		{
			latencies = new StatsList(isEstimate);
			latencies.add(latency);
			m_remotePartitionNetworkLatencies.put(partition, latencies);
		}
	}
	
	public int getMedianLatency() {
		return m_latencies.getMedian();
	}
	
	public int getMedianRemotePartitionNetworkLatency(int partition)
	{
		StatsList latencies = m_remotePartitionNetworkLatencies.get(partition);
		
		if(latencies != null)
		{
			return latencies.getMedian();
		}
		
		return 0;
	}
	
	public int getLocalLatency()
	{
		if(m_localLatency < 0)
		{
			int maxRemotePartitionNetworkLatency = Integer.MIN_VALUE;
			
			for(Entry<Integer, StatsList> e : m_remotePartitionNetworkLatencies.entrySet())
			{
				StatsList latencies = e.getValue();
				
				if(!latencies.isEstimates() && latencies.getMedian() > maxRemotePartitionNetworkLatency)
				{
					maxRemotePartitionNetworkLatency = latencies.getMedian();
				}
			}
			
			m_localLatency = Math.max(getMedianLatency() - maxRemotePartitionNetworkLatency, 0);
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
