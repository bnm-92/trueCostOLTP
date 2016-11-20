package org.voltdb.repartitioner;

import java.util.Arrays;

/**
 * <p>
 * Key for grouping a set of transactions into a single set of statistics for
 * use in the model. Key consists of:
 * </p>
 * <ul>
 * <li>Stored Procedure</li>
 * <li>Initiator</li>
 * <li>Partitions Used</li>
 * </ul>
 */
public class TxnGroupStatsKey {
	/**
	 * Stored procedure.
	 */
	private String m_procedureName;

	/**
	 * Host id for the initiator.
	 */
	private int m_initiatorHostId;

	/**
	 * Partition(s) used.
	 */
	private int m_partitions[];

	/**
	 * Cached hash code.
	 */
	private int m_hashCode;

	/**
	 * Constructor for group of single-partition transactions accessing the
	 * given partition with the given initiator.
	 * 
	 * @param procedureName
	 * @param initiatorHostId
	 * @param partition
	 */
	public TxnGroupStatsKey(String procedureName, int initiatorHostId, int partition) {
		m_procedureName = procedureName;
		m_initiatorHostId = initiatorHostId;
		m_partitions = new int[] { partition };
		m_hashCode = calcHashCode();
	}

	/**
	 * Constructor for group of multi-partition transactions accessing the given
	 * partitions with the given initiator.
	 * 
	 * @param procedureName
	 * @param initiatorHostId
	 * @param partition
	 */
	public TxnGroupStatsKey(String procedureName, int initiatorHostId, int[] partitions) {
		assert (partitions != null);
		assert (partitions.length > 0);

		m_procedureName = procedureName;
		m_initiatorHostId = initiatorHostId;
		m_partitions = partitions.clone();
		m_hashCode = calcHashCode();
	}

	public String getProcedureName() {
		return m_procedureName;
	}

	public int getInitiatorHostId() {
		return m_initiatorHostId;
	}

	public int[] getPartitions() {
		return m_partitions;
	}

	public boolean isSinglePartition() {
		return m_partitions.length == 1;
	}

	public int calcHashCode() {
		int hash = 0;
		int prime = 23;

		hash = m_procedureName.hashCode();
		hash = prime * hash + m_initiatorHostId;
		hash = prime * hash + Arrays.hashCode(m_partitions);

		return hash;
	}

	@Override
	public int hashCode() {
		return m_hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TxnGroupStatsKey)) {
			return false;
		}

		TxnGroupStatsKey key = (TxnGroupStatsKey) obj;

		if (!key.m_procedureName.equals(m_procedureName)) {
			return false;
		}

		if (key.m_initiatorHostId != m_initiatorHostId) {
			return false;
		}

		if (key.m_partitions.length != m_partitions.length) {
			return false;
		}

		// Expect transactions to use a small number of partitions, two nested
		// loops ok.
		loopOverPartitions: for (int i = 0; i < m_partitions.length; ++i) {
			for (int j = 0; j < key.m_partitions.length; ++j) {
				if (m_partitions[i] == key.m_partitions[j]) {
					continue loopOverPartitions;
				}
			}

			return false;
		}

		return true;
	}
}
