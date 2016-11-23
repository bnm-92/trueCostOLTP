package org.voltdb.repartitioner;

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
	 * All partition ids.
	 * In ILP, partition i is the i^th partition in the array.
	 */
	private int[] m_allPartitions;
	
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
	 * Matrix of variables p_ij.
	 * 1 <= i <= numSites
	 * 1 <= j <= numHosts
	 * p_ij = 1 iff partition i is assigned to host j
	 */
	private String[][] m_partitionAssignmentVars;

	/**
	 * Constructor for a PartitioningGenerator for a cluster with the given
	 * number of sites and hosts.
	 * 
	 * @param numSites
	 * @param numHosts
	 */
	public PartitioningGenerator(int[] allPartitions, int numHosts, int maxPartitionsPerHost) {
		m_solverFactory = new SolverFactoryGLPK();
		
		assert (allPartitions != null);
		assert (allPartitions.length > 0);
		
		m_allPartitions = allPartitions.clone();
		m_numPartitions = allPartitions.length;
		
		assert (numHosts * maxPartitionsPerHost >= m_numPartitions);

		m_numHosts = numHosts;
		m_maxPartitionsPerHost = maxPartitionsPerHost;
		
		createPartitionAssignmentVars();
	}
	
	private String makeVariable(StringBuilder sb, String prefix, int index)
	{
		String variable = null;
		
		sb.append(prefix);
		sb.append('_');
		sb.append(index);
		
		variable = sb.toString();
		sb.delete(0, variable.length());
		
		return variable;
	}
	
	private String makeVariable(StringBuilder sb, String prefix, int index1, int index2)
	{
		String variable = null;
		
		sb.append(prefix);
		sb.append('_');
		sb.append(index1);
		sb.append(index2);
		
		variable = sb.toString();
		sb.delete(0, variable.length());
		
		return variable;
	}
	
	private void createPartitionAssignmentVars()
	{
		StringBuilder sb = new StringBuilder();
		
		m_partitionAssignmentVars = new String[m_numPartitions][m_numHosts];
		
		for(int i = 1; i <= m_numPartitions; ++i)
		{
			for(int j = 1; j <= m_numHosts; ++j)
			{
				m_partitionAssignmentVars[i-1][j-1] = makeVariable(sb, "p", i, j);
			}
		}
	}
	
	private void addPartitionAssignmentConstraints()
	{
		// Add binary variables p_ij
		for(int i = 0; i < m_numPartitions; ++i)
		{
			for(int j = 0; j < m_numHosts; ++j)
			{
				m_ilp.setVarType(m_partitionAssignmentVars[i][j], VarType.BOOL);
			}
		}
		
		// Add constraints so that each partition is only assigned to one host
		for(int i = 0; i < m_numPartitions; ++i)
		{
			Linear constraintLHS = new Linear();
			
			for(int j = 0; j < m_numHosts; ++j)
			{
				constraintLHS.add(1, m_partitionAssignmentVars[i][j]);
			}
			
			m_ilp.add(constraintLHS, Operator.EQ, 1);
		}
		
		// Add constraints so that each host has less than equal to the maximum number of partitions per host
		for(int j = 0; j < m_numHosts; ++j)
		{
			Linear constraintLHS = new Linear();
			
			for(int i = 0; i < m_numPartitions; ++i)
			{
				constraintLHS.add(1, m_partitionAssignmentVars[i][j]);
			}
			
			m_ilp.add(constraintLHS, Operator.LE, m_maxPartitionsPerHost);
		}
	}
}
