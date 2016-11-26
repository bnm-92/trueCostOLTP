package org.voltdb.repartitioner;

import java.util.Random;

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

	public void testLatencies() {
		int[] allPartitionIds = { 101, 102, 201, 202 };

		WorkloadSampleStats sample = new WorkloadSampleStats();

		for (int i = 0; i < 2000; ++i) {
			sample.addSinglePartitionTransaction("Proc1", 100, 101, getRandom(2, 3));
			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc1", 100, 101, getRandom(9, 11),
					true);
		}

		for (int i = 0; i < 1000; ++i) {
			sample.addSinglePartitionTransaction("Proc1", 200, 101, getRandom(11, 14));
			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc1", 200, 101, getRandom(9, 11),
					false);
		}
		
		for (int i = 0; i < 100; ++i) {
			sample.addSinglePartitionTransaction("Proc2", 100, 102, getRandom(4, 5));
			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc2", 100, 102, getRandom(40, 50),
					true);
		}
		
		for (int i = 0; i < 50; ++i) {
			sample.addMultiPartitionTransaction("Proc3", 200, getRandom(112, 170));
			sample.recordMultiPartitionTransactionRemotePartitionNetworkLatency("Proc3", 200, 102, getRandom(100, 150),
					false);
			sample.recordMultiPartitionTransactionRemotePartitionNetworkLatency("Proc3", 200, 201, getRandom(100, 150),
					true);
		}
		
		for (int i = 0; i < 800; ++i) {
			sample.addSinglePartitionTransaction("Proc2", 100, 102, getRandom(2, 3));
			sample.recordSinglePartitionTransactionRemotePartitionNetworkLatency("Proc2", 100, 102, getRandom(20, 30),
					true);
		}

		printStats(sample.getSinglePartitionTxnStats().get(0), allPartitionIds);
		printStats(sample.getSinglePartitionTxnStats().get(1), allPartitionIds);
		printStats(sample.getSinglePartitionTxnStats().get(2), allPartitionIds);
		printStats(sample.getSinglePartitionTxnStats().get(3), allPartitionIds);
		printStats(sample.getMultiPartitionTxnStats().get(0), allPartitionIds);
	}

}
