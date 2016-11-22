package org.voltdb.repartitioner;

/**
 * Generate the optimum partitioning based on statistics obtained from a workload sample.
 *
 */
public class PartitioningGenerator {
	/**
	 * Number of sites in the cluster.
	 */
	private int m_numSites;
	
	/**
	 * Number of hosts in cluster.
	 */
	private int m_numHosts;
	
	public PartitioningGenerator(int numSites, int numHosts)
	{
		assert(numSites > 0);
		assert(numHosts > 0);
		
		m_numSites = numSites;
		m_numHosts = numHosts;
	}
}
