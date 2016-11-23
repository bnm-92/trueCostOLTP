package org.voltdb.repartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.javailp.Linear;
import net.sf.javailp.Operator;
import net.sf.javailp.Problem;
import net.sf.javailp.SolverFactory;
import net.sf.javailp.SolverFactoryGLPK;
import net.sf.javailp.VarType;

/**
 * Generate the optimum partitioning based on statistics obtained from a
 * workload sample.
 *
 */
public class PartitioningGenerator {

	/**
	 * Factory for instances of the ILP solver.
	 */
	private SolverFactory m_solverFactory;

	/**
	 * Map from host-id to host index (h_1, ..., h_m) in the ILP.
	 */
	private Map<Integer, Integer> m_hostIdToIndex = new HashMap<Integer, Integer>();

	/**
	 * Map from host index in ILP to host-id.
	 */
	private Map<Integer, Integer> m_hostIndexToId = new HashMap<Integer, Integer>();

	/**
	 * Map from partition-id to partition index (p_1, ..., p_n) in the ILP.
	 */
	private Map<Integer, Integer> m_partitionIdToIndex = new HashMap<Integer, Integer>();

	/**
	 * Map from partition index to partition-id in the ILP.
	 */
	private Map<Integer, Integer> m_partitionIndexToId = new HashMap<Integer, Integer>();

	/**
	 * Map from a host-id to a list of its site-id's.
	 */
	private Map<Integer, ArrayList<Integer>> m_hostIdToSiteIds;

	/**
	 * Matrix of variables p_ij : p_ij = 1 iff partition i is assigned to host j
	 */
	private String[][] m_partitionAssignmentVariables;

	/**
	 * Partition Ids.
	 */
	private int[] m_allPartitionIds;

	/**
	 * Number of partitions.
	 */
	private int m_numPartitions;

	/**
	 * Number of hosts in cluster.
	 */
	private int m_numHosts;

	/**
	 * Maximum number of partitions per host.
	 */
	private int m_maxPartitionsPerHost;

	/**
	 * ILP problem to solve to generate partitioning.
	 */
	private Problem m_ilp;

	/**
	 * Use a StringBuilder to make variable names for ILP for efficiency.
	 */
	private StringBuilder m_varNameBuilder = new StringBuilder();

	public PartitioningGenerator(int[] allHostIds, int[] allPartitionIds,
			Map<Integer, ArrayList<Integer>> hostToSiteIds, int maxPartitionsPerHost) {
		m_solverFactory = new SolverFactoryGLPK();

		assert (allHostIds != null);
		assert (allHostIds.length > 0);

		m_numHosts = allHostIds.length;
		for (int i = 0; i < m_numHosts; ++i) {
			m_hostIdToIndex.put(allHostIds[i], i + 1);
			m_hostIndexToId.put(i + 1, allHostIds[i]);
		}

		assert (allPartitionIds != null);
		assert (allPartitionIds.length > 0);

		m_allPartitionIds = allPartitionIds.clone();
		m_numPartitions = allPartitionIds.length;
		for (int i = 0; i < m_numPartitions; ++i) {
			m_partitionIdToIndex.put(allPartitionIds[i], i + 1);
			m_partitionIndexToId.put(i + 1, allPartitionIds[i]);
		}

		m_maxPartitionsPerHost = maxPartitionsPerHost;
		assert (m_numHosts * m_maxPartitionsPerHost >= m_numPartitions);

		assert (hostToSiteIds != null);
		assert (hostToSiteIds.size() == m_numHosts);
		m_hostIdToSiteIds = hostToSiteIds;

		createPartitionAssignmentVars();
	}

	public Map<Integer, ArrayList<Integer>> findOptimumPartitioning(WorkloadSampleStats sample) {
		int latencyVariableIndex = 1;
		int maxVariableIndex = 1;
		Linear constraintLHS = null;
		int partitionIndex = 1;
		int hostIndex = 1;

		m_ilp = new Problem();

		// Add binary variables p_ij
		for (int i = 0; i < m_numPartitions; ++i) {
			for (int j = 0; j < m_numHosts; ++j) {
				m_ilp.setVarType(m_partitionAssignmentVariables[i][j], VarType.BOOL);
			}
		}

		// Add constraints so that each partition is only assigned to one host
		for (int i = 0; i < m_numPartitions; ++i) {
			constraintLHS = new Linear();

			for (int j = 0; j < m_numHosts; ++j) {
				constraintLHS.add(1, m_partitionAssignmentVariables[i][j]);
			}

			m_ilp.add(constraintLHS, Operator.EQ, 1);
		}

		// Add constraints so that each host has less than equal to the maximum
		// number of partitions per host
		for (int j = 0; j < m_numHosts; ++j) {
			constraintLHS = new Linear();

			for (int i = 0; i < m_numPartitions; ++i) {
				constraintLHS.add(1, m_partitionAssignmentVariables[i][j]);
			}

			m_ilp.add(constraintLHS, Operator.GE, 0);
			m_ilp.add(constraintLHS, Operator.LE, m_maxPartitionsPerHost);
		}

		// Add constraints for single partition transactions
		for (TxnGroupStats groupStats : sample.getSinglePartitionTxnStats()) {
			String latencyVariable = makeVariable("lat", latencyVariableIndex++);

			// Introduce a new latency variable
			m_ilp.setVarType(latencyVariable, VarType.REAL);
			m_ilp.setVarLowerBound(latencyVariable, 0);
			groupStats.setLatencyVariable(latencyVariable);

			// Add constraint for latency
			constraintLHS = new Linear();
			partitionIndex = m_partitionIdToIndex.get(groupStats.getPartition());
			hostIndex = m_hostIdToIndex.get(groupStats.getInitiatorHostId());
			int totalLocalLatency = groupStats.getNumTransactions() * groupStats.getLocalLatency();
			int totalRemoteLatency = groupStats.getNumTransactions()
					* groupStats.getMedianRemotePartitionNetworkLatency(groupStats.getPartition());

			constraintLHS.add(1, latencyVariable);
			constraintLHS.add(totalRemoteLatency, m_partitionAssignmentVariables[partitionIndex][hostIndex]);

			m_ilp.add(constraintLHS, Operator.EQ, totalLocalLatency + totalRemoteLatency);
		}

		// Add constraints for multi partition transactions
		for (TxnGroupStats groupStats : sample.getMultiPartitionTxnStats()) {
			String latencyVariable = makeVariable("lat", latencyVariableIndex++);
			String maxVariable = makeVariable("max", maxVariableIndex++);

			// Introduce a new latency variable
			m_ilp.setVarType(latencyVariable, VarType.REAL);
			m_ilp.setVarLowerBound(latencyVariable, 0);
			groupStats.setLatencyVariable(latencyVariable);
			
			// Introduce a new maximum variable
			m_ilp.setVarType(maxVariable, VarType.REAL);
			m_ilp.setVarLowerBound(maxVariable, 0);
			groupStats.setLatencyVariable(maxVariable);
			
			// Add constraints to determine the maximum of all remote partition latencies
			hostIndex = m_hostIdToIndex.get(groupStats.getInitiatorHostId());
			for(int partition : m_allPartitionIds)
			{
				if(groupStats.getMedianRemotePartitionNetworkLatency(partition) != 0)
				{
					partitionIndex = m_partitionIdToIndex.get(partition);
					int totalRemoteLatency = groupStats.getNumTransactions()
							* groupStats.getMedianRemotePartitionNetworkLatency(partition);
					
					constraintLHS = new Linear();
					
					constraintLHS.add(totalRemoteLatency, m_partitionAssignmentVariables[partitionIndex][hostIndex]);
					constraintLHS.add(1, maxVariable);
					
					m_ilp.add(constraintLHS, Operator.GE, totalRemoteLatency);
				}
			}
			
			// Add constraint for latency
			int totalLocalLatency = groupStats.getNumTransactions() * groupStats.getLocalLatency();
			
			constraintLHS = new Linear();
			constraintLHS.add(1, latencyVariable);
			constraintLHS.add(-1, maxVariable);
			
			m_ilp.add(constraintLHS, Operator.EQ, totalLocalLatency);
		}

		return null;
	}

	private String makeVariable(String prefix, int index) {
		String variable = null;

		m_varNameBuilder.append(prefix);
		m_varNameBuilder.append('_');
		m_varNameBuilder.append(index);

		variable = m_varNameBuilder.toString();
		m_varNameBuilder.delete(0, variable.length());

		return variable;
	}

	private String makeVariable(String prefix, int index1, int index2) {
		String variable = null;

		m_varNameBuilder.append(prefix);
		m_varNameBuilder.append('_');
		m_varNameBuilder.append(index1);
		m_varNameBuilder.append(index2);

		variable = m_varNameBuilder.toString();
		m_varNameBuilder.delete(0, variable.length());

		return variable;
	}

	private void createPartitionAssignmentVars() {
		m_partitionAssignmentVariables = new String[m_numPartitions][m_numHosts];

		for (int i = 1; i <= m_numPartitions; ++i) {
			for (int j = 1; j <= m_numHosts; ++j) {
				m_partitionAssignmentVariables[i - 1][j - 1] = makeVariable("p", i, j);
			}
		}
	}
}