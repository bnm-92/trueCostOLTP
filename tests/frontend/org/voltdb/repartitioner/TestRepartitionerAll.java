package org.voltdb.repartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestRepartitionerAll extends TestCase {

	private Random m_random = new Random(System.currentTimeMillis());

	private int getRandom(int min, int max) {
		assert (max >= min);

		return m_random.nextInt(max - min) + min;
	}

	private void printStats(TxnGroupStats groupStats, int[] partitionIds) {
		System.out.println(groupStats.toString() + ":");
		System.out.println("\tmedian latency: " + groupStats.getMedianLatency());
		System.out.println("\tlocal latency: " + groupStats.getLocalLatency());

		for (int partitionId : partitionIds) {
			System.out.println("\tsite " + partitionId + " latency: "
					+ groupStats.getMedianRemotePartitionNetworkLatency(partitionId)
					+ (groupStats.isRemotePartitionNetworkLatencyEstimate(partitionId) ? " (estimate)" : " (actual)"));
		}
	}
	
	private void printSchedule(TxnScheduleGraph graph)
	{
		TxnScheduleGraph.BaseNode currNode = graph.getNodeChain().getHeadNode();
		
		if(currNode != null)
		{
			while(currNode != null)
			{
				if(currNode instanceof TxnScheduleGraph.SerialNode)
				{
					TxnScheduleGraph.SerialNode snode = (TxnScheduleGraph.SerialNode) currNode;
					
					System.out.println("serial node: " + snode.getTxnGroupStats().toString());
				}
				else
				{
					TxnScheduleGraph.ConcurrentNode cnode = (TxnScheduleGraph.ConcurrentNode) currNode;
					
					System.out.println("concurrent node: ");
					
					for (TxnScheduleGraph.NodeChain nodeChain : cnode.getNodeChains()) {
						System.out.println("\tserial nodes: ");
						
						TxnScheduleGraph.SerialNode currSerialNode = (TxnScheduleGraph.SerialNode) nodeChain.getHeadNode();
						
						while(currSerialNode != null)
						{
							System.out.println("\t\tserial node: " + currSerialNode.getTxnGroupStats().toString());
							currSerialNode = (TxnScheduleGraph.SerialNode) currSerialNode.getNextNode();
						}
					}
				}
				
				currNode = currNode.getNextNode();
			}
		}
		else
		{
			System.out.println("empty schedule");
		}
	}
	
	private void printPartitioning(Map<Integer, ArrayList<Integer>> p)
	{
		System.out.println("Partitioning Result: ");
		
		for(Entry<Integer, ArrayList<Integer>> hostPartitions : p.entrySet())
		{
			StringBuilder sb = new StringBuilder();
			
			sb.append("\t");
			sb.append(hostPartitions.getKey());
			sb.append(" : { ");
			
			for(int i = 0; i < hostPartitions.getValue().size(); ++i)
			{
				if(i > 0) sb.append(", ");
				sb.append(hostPartitions.getValue().get(i));
			}
			
			sb.append(" }");
			
			System.out.println(sb.toString());
		}
	}

//	public void testLatencies() {
//		int[] allPartitionIds = { 101, 102, 201, 202 };
//
//		WorkloadSampleStats sample = new WorkloadSampleStats();
//
//		for (int i = 0; i < 2000; ++i) {
//			sample.addSinglePartitionTransaction("Proc1", 100, 101, getRandom(2, 3));
//			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc1", 100, 101, getRandom(9, 11),
//					true);
//		}
//
//		for (int i = 0; i < 1000; ++i) {
//			sample.addSinglePartitionTransaction("Proc1", 200, 101, getRandom(11, 14));
//			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc1", 200, 101, getRandom(9, 11),
//					false);
//		}
//		
//		for (int i = 0; i < 100; ++i) {
//			sample.addSinglePartitionTransaction("Proc2", 100, 102, getRandom(4, 5));
//			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc2", 100, 102, getRandom(40, 50),
//					true);
//		}
//		
//		for (int i = 0; i < 50; ++i) {
//			sample.addMultiPartitionTransaction("Proc3", 200, getRandom(112, 170));
//			sample.recordMultiPartitionTransactionRemotePartitionNetworkLatency("Proc3", 200, 102, getRandom(100, 150),
//					false);
//			sample.recordMultiPartitionTransactionRemotePartitionNetworkLatency("Proc3", 200, 201, getRandom(100, 150),
//					true);
//		}
//		
//		for (int i = 0; i < 800; ++i) {
//			sample.addSinglePartitionTransaction("Proc2", 100, 102, getRandom(2, 3));
//			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc2", 100, 102, getRandom(20, 30),
//					true);
//		}
//
//		printStats(sample.getSinglePartitionTxnStats().get(0), allPartitionIds);
//		printStats(sample.getSinglePartitionTxnStats().get(1), allPartitionIds);
//		printStats(sample.getSinglePartitionTxnStats().get(2), allPartitionIds);
//		printStats(sample.getSinglePartitionTxnStats().get(3), allPartitionIds);
//		printStats(sample.getMultiPartitionTxnStats().get(0), allPartitionIds);
//	}
//
//	public void testSchedule()
//	{
//		WorkloadSampleStats sample = new WorkloadSampleStats();
//		
//		sample.addSinglePartitionTransaction("Proc1", 100, 101, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc2", 200, 102, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc2", 100, 201, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc3", 100, 102, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc1", 200, 202, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc3", 100, 201, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc2", 200, 102, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc1", 200, 101, getRandom(2, 3));
//		
//		sample.addMultiPartitionTransaction("Proc1", 100, getRandom(10, 20));
//		sample.addMultiPartitionTransaction("Proc4", 100, getRandom(10, 20));
//		sample.addMultiPartitionTransaction("Proc5", 200, getRandom(10, 20));
//		sample.addMultiPartitionTransaction("Proc4", 200, getRandom(10, 20));
//		sample.addMultiPartitionTransaction("Proc4", 100, getRandom(10, 20));
//		sample.addMultiPartitionTransaction("Proc5", 200, getRandom(10, 20));
//		
//		sample.addSinglePartitionTransaction("Proc1", 100, 101, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc6", 200, 102, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc2", 100, 201, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc7", 100, 102, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc1", 200, 202, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc6", 100, 201, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc7", 200, 102, getRandom(2, 3));
//		sample.addSinglePartitionTransaction("Proc1", 200, 101, getRandom(2, 3));
//		
//		System.out.println("Best Schedule (" + Math.round(TxnScheduleGraph.getBestCaseScheduleProbability(sample) * 100) + "%)");
//		printSchedule(TxnScheduleGraph.getBestCaseSchedule(sample));
//		
//		System.out.println("Worst Schedule (" + Math.round((1 - TxnScheduleGraph.getBestCaseScheduleProbability(sample)) * 100) + "%)");
//		printSchedule(TxnScheduleGraph.getWorstCaseSchedule(sample));
//	}
	
	public void testPartitioningGenerator()
	{
		int[] allHostIds = new int[] { 100, 200 };
		int[] allPartitionIds = new int[] { 101, 102, 201, 202 };
		
		Map<Integer, ArrayList<Integer>> hostToPartitionsMap = new HashMap<Integer, ArrayList<Integer>>();
		ArrayList<Integer> partitions = new ArrayList<Integer>();
		
		partitions.add(101);
		partitions.add(102);
		hostToPartitionsMap.put(100, partitions);
		partitions.clear();
		partitions.add(201);
		partitions.add(202);
		hostToPartitionsMap.put(200, partitions);
		
		PartitioningGenerator gen = new PartitioningGenerator(allHostIds, allPartitionIds, 3);
		WorkloadSampleStats sample = new WorkloadSampleStats();
		
		sample.addSinglePartitionTransaction("Proc1", 100, 101, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc2", 200, 102, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc2", 100, 201, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc3", 100, 102, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc1", 200, 202, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc3", 100, 201, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc2", 200, 102, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc1", 200, 101, getRandom(2, 3));
		
		sample.addMultiPartitionTransaction("Proc1", 100, getRandom(10, 20));
		sample.addMultiPartitionTransaction("Proc4", 100, getRandom(10, 20));
		sample.addMultiPartitionTransaction("Proc5", 200, getRandom(10, 20));
		sample.addMultiPartitionTransaction("Proc4", 200, getRandom(10, 20));
		sample.addMultiPartitionTransaction("Proc4", 100, getRandom(10, 20));
		sample.addMultiPartitionTransaction("Proc5", 200, getRandom(10, 20));
		
		sample.addSinglePartitionTransaction("Proc1", 100, 101, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc6", 200, 102, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc2", 100, 201, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc7", 100, 102, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc1", 200, 202, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc6", 100, 201, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc7", 200, 102, getRandom(2, 3));
		sample.addSinglePartitionTransaction("Proc1", 200, 101, getRandom(2, 3));
		
		PartitioningGeneratorResult partitioningGeneratorResult = gen.findOptimumPartitioning(sample);
		Assert.assertNotNull(partitioningGeneratorResult);
		
		Map<Integer, ArrayList<Integer>> partitioningMapping = partitioningGeneratorResult.getHostToPartitionsMap();
		Assert.assertNotNull(partitioningMapping);
		
		System.out.println("Estimated workload execution time on current partitioning: " + TxnScheduleGraph.getEstimatedExecutionTime(sample, hostToPartitionsMap));
		System.out.println("Estimated workload execution time on optimum partitioning: " + partitioningGeneratorResult.getEstimatedExecTime());
		
		printPartitioning(partitioningMapping);
	}
	
//	public void testPartitioningGeneratorLarge()
//	{
//		int[] allHostIds = new int[] { 100, 200 };
//		int[] allPartitionIds = new int[] { 101, 102, 201, 202 };
//		
//		Map<Integer, ArrayList<Integer>> hostToPartitionsMap = new HashMap<Integer, ArrayList<Integer>>();
//		ArrayList<Integer> partitions = new ArrayList<Integer>();
//		
//		partitions.add(101);
//		partitions.add(102);
//		hostToPartitionsMap.put(100, partitions);
//		partitions.clear();
//		partitions.add(201);
//		partitions.add(202);
//		hostToPartitionsMap.put(200, partitions);
//		
//		PartitioningGenerator gen = new PartitioningGenerator(allHostIds, allPartitionIds, 2);
//		WorkloadSampleStats sample = new WorkloadSampleStats(1000);
//		
//		for (int i = 0; i < 200000; ++i) {
//			sample.addSinglePartitionTransaction("Proc1", 200, 101, getRandom(2, 3));
//			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc1", 200, 101, getRandom(9, 11),
//					true);
//		}
//
//		for (int i = 0; i < 100000; ++i) {
//			sample.addSinglePartitionTransaction("Proc1", 200, 101, getRandom(11, 14));
//			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc1", 200, 101, getRandom(9, 11),
//					false);
//		}
//		
//		for (int i = 0; i < 100000; ++i) {
//			sample.addSinglePartitionTransaction("Proc2", 200, 102, getRandom(4, 5));
//			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc2", 200, 102, getRandom(40, 50),
//					true);
//		}
//		
//		for (int i = 0; i < 50000; ++i) {
//			sample.addMultiPartitionTransaction("Proc3", 200, getRandom(112, 170));
//			sample.recordMultiPartitionTransactionRemotePartitionNetworkLatency("Proc3", 200, 102, getRandom(100, 150),
//					false);
//			sample.recordMultiPartitionTransactionRemotePartitionNetworkLatency("Proc3", 200, 101, getRandom(100, 150),
//					true);
//		}
//		
//		for (int i = 0; i < 80000; ++i) {
//			sample.addSinglePartitionTransaction("Proc2", 200, 102, getRandom(2, 3));
//			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc2", 200, 102, getRandom(20, 30),
//					true);
//		}
//		
//		PartitioningGeneratorResult partitioningGeneratorResult = gen.findOptimumPartitioning(sample);
//		Assert.assertNotNull(partitioningGeneratorResult);
//		
//		Map<Integer, ArrayList<Integer>> partitioningMapping = partitioningGeneratorResult.getHostToPartitionsMap();
//		Assert.assertNotNull(partitioningMapping);
//		
//		System.out.println("Estimated workload execution time on current partitioning: " + TxnScheduleGraph.getEstimatedExecutionTime(sample, hostToPartitionsMap));
//		System.out.println("Estimated workload execution time on optimum partitioning: " + partitioningGeneratorResult.getEstimatedExecTime());
//		
//		printPartitioning(partitioningMapping);
//	}
}
