package org.voltdb.repartitioner;

import java.util.ArrayList;
import java.util.Map;

public class PartitioningGeneratorResult {
	/**
	 * Map from host-id to a list of partition id's giving the optimum partitioning
	 * as determined by the ILP.
	 */
	private Map<Integer, ArrayList<Integer>> m_hostToPartitionsMap;
	
	/**
	 * Estimated execution time of the workload (input to the ILP) in the optimum partitioning.
	 */
	private double m_estimatedExecTime = -1;
	
	public PartitioningGeneratorResult() {}
	
	public PartitioningGeneratorResult(Map<Integer, ArrayList<Integer>> hostToPartitionsMap, double estimatedExecTime) {
		m_hostToPartitionsMap = hostToPartitionsMap;
		m_estimatedExecTime = estimatedExecTime;
	}
	
	public Map<Integer, ArrayList<Integer>> getHostToPartitionsMap() {
		return m_hostToPartitionsMap;
	}
	
	public double getEstimatedExecTime() {
		return m_estimatedExecTime;
	}
}
