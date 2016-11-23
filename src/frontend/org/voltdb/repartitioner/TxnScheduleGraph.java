package org.voltdb.repartitioner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Graph representing the execution schedule of a set of transactions. Graph is
 * based on two assumptions:
 * </p>
 * <ul>
 * <li>Single partition transactions at different partitions execute
 * concurrently</li>
 * <li>Multi-partition transaction will block execution of all other
 * transactions until completion</li>
 * </ul>
 *
 */
public class TxnScheduleGraph {

	public static class BaseNode {
		private BaseNode m_prevNode;
		private BaseNode m_nextNode;

		public BaseNode getPrevNode() {
			return m_prevNode;
		}

		public void setPrevNode(BaseNode m_prevNode) {
			this.m_prevNode = m_prevNode;
		}

		public BaseNode getNextNode() {
			return m_nextNode;
		}

		public void setNextNode(BaseNode m_nextNode) {
			this.m_nextNode = m_nextNode;
		}
	}

	/**
	 * Node representing (serially executing) transaction(s) which executes
	 * after the previous node and before the next node.
	 */
	public static class SerialNode extends BaseNode {
		private TxnGroupStats m_txnGroupStats;

		public SerialNode(TxnGroupStats txnGroupStats) {
			m_txnGroupStats = txnGroupStats;
		}

		public TxnGroupStats getTxnGroupStats() {
			return m_txnGroupStats;
		}
	}

	public static class NodeChain {
		private BaseNode m_headNode;
		private BaseNode m_tailNode;
		private int m_length;

		public void addNode(BaseNode node) {
			if (m_tailNode != null) {
				m_tailNode.setNextNode(node);
				node.setPrevNode(m_tailNode);
				m_tailNode = node;
			} else {
				m_headNode = m_tailNode = node;
			}

			++m_length;
		}

		public BaseNode getHeadNode() {
			return m_headNode;
		}

		public BaseNode getTailNode() {
			return m_tailNode;
		}

		public int getLength() {
			return m_length;
		}
		
		public boolean isEmpty()
		{
			return m_length == 0;
		}
	}

	/**
	 * Node representing (concurrently executing) transaction(s) (or series of
	 * serially executing transaction(s)) which execute after the previous node
	 * and before the next node.
	 */
	public static class ConcurrentNode extends BaseNode {
		private Map<Integer, NodeChain> m_concurrentNodes = new HashMap<Integer, NodeChain>();

		public void addNode(SerialNode node) {
			assert (node.getTxnGroupStats() != null);
			assert (node.getTxnGroupStats().isSinglePartition());

			int partition = node.getTxnGroupStats().getPartition();
			NodeChain partitionChain = m_concurrentNodes.get(partition);

			if (partitionChain != null) {
				partitionChain.addNode(node);
			} else {
				partitionChain = new NodeChain();
				partitionChain.addNode(node);
				m_concurrentNodes.put(partition, partitionChain);
			}
		}
		
		public Collection<NodeChain> getNodeChains()
		{
			return m_concurrentNodes.values();
		}
	}

	private NodeChain m_nodeChain = new NodeChain();
	
	public NodeChain getNodeChain()
	{
		return m_nodeChain;
	}
	
	public static float getBestCaseScheduleProbability(WorkloadSampleStats sample)
	{
		if(sample.getSinglePartitionTxnStats().isEmpty() && sample.getMultiPartitionTxnStats().isEmpty()) {
			return 0;
		}
		
		return (float) sample.getSinglePartitionTxnStats().size() / (float) (sample.getSinglePartitionTxnStats().size() +  sample.getMultiPartitionTxnStats().size());
	}

	/**
	 * Get the best case schedule (lowest execution time). This is simply the
	 * schedule with all single-partition transactions first then all
	 * multi-partitions.
	 * 
	 * @param sample
	 * @return
	 */
	public static TxnScheduleGraph getBestCaseSchedule(WorkloadSampleStats sample) {
		TxnScheduleGraph graph = new TxnScheduleGraph();

		if (!sample.getSinglePartitionTxnStats().isEmpty()) {
			graph.m_nodeChain.addNode(new ConcurrentNode());
			
			for (TxnGroupStats groupStats : sample.getSinglePartitionTxnStats()) {
				((ConcurrentNode) graph.m_nodeChain.getTailNode()).addNode(new SerialNode(groupStats));
			}
		}

		if (!sample.getMultiPartitionTxnStats().isEmpty()) {
			for (TxnGroupStats groupStats : sample.getMultiPartitionTxnStats()) {
				graph.m_nodeChain.addNode(new SerialNode(groupStats));
			}
		}

		return graph;
	}

	/**
	 * Get the worst case schedule (highest execution time).
	 * 
	 * @param sample
	 * @return
	 */
	public static TxnScheduleGraph getWorstCaseSchedule(WorkloadSampleStats sample) {
		TxnScheduleGraph graph = new TxnScheduleGraph();
		
		Collections.sort(sample.getSinglePartitionTxnStats());
		Collections.reverse(sample.getSinglePartitionTxnStats());
		Collections.sort(sample.getMultiPartitionTxnStats());
		Collections.reverse(sample.getMultiPartitionTxnStats());
		
		int i = 0;
		int numSPTxns = sample.getSinglePartitionTxnStats().size();
		int j = 0;
		int numMPTxns = sample.getMultiPartitionTxnStats().size();
		
		while(i < numSPTxns && j < numMPTxns)
		{
			graph.m_nodeChain.addNode(new SerialNode(sample.getSinglePartitionTxnStats().get(i)));
			graph.m_nodeChain.addNode(new SerialNode(sample.getMultiPartitionTxnStats().get(j)));
			++i;
			++j;
		}
		
		while(i < numSPTxns)
		{
			graph.m_nodeChain.addNode(new ConcurrentNode());
			((ConcurrentNode) graph.m_nodeChain.getTailNode()).addNode(new SerialNode(sample.getSinglePartitionTxnStats().get(i)));
			++i;
		}
		
		while(j < numMPTxns)
		{
			graph.m_nodeChain.addNode(new SerialNode(sample.getMultiPartitionTxnStats().get(j)));
			++j;
		}
		
		return graph;
	}
}
