package org.voltdb.repartitioner;

import junit.framework.TestCase;

import org.voltdb.repartitioner.PartitioningSolver;

public class TestPartitioningSolver extends TestCase {

	public void testGeneratePartitioning()
	{
		PartitioningSolver solver = new PartitioningSolver();
		
		solver.generatePartitioning();
	}
	
}
