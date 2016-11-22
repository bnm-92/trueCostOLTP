package org.voltdb.repartitioner;

/**
 * <p>
 * Key for grouping a set of transactions into a single set of statistics for
 * use in the model. Key consists of:
 * </p>
 * <ul>
 * <li>Stored Procedure</li>
 * <li>Initiator</li>
 * <li>Single-Partitioned or Multi-(Every)-Partition</li>
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
	 * Single-partitioned transactions? (Every-partition otherwise).
	 */
	private boolean m_isSinglePartition;

	/**
	 * Partition used.
	 */
	private int m_partition;

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
		m_partition = partition;
		m_isSinglePartition = true;
		m_hashCode = calcHashCode();
	}

	/**
	 * Constructor for group of multi-partition transactions accessing every
	 * partitions with the given initiator.
	 * 
	 * @param procedureName
	 * @param initiatorHostId
	 */
	public TxnGroupStatsKey(String procedureName, int initiatorHostId) {
		m_procedureName = procedureName;
		m_initiatorHostId = initiatorHostId;
		m_isSinglePartition = false;
		m_hashCode = calcHashCode();
	}
	
	/**
	 * Reset the key to be for a group of single-partition transactions
	 * with the given stored procedure, initiator host id and partition.
	 */
	public void reset(String procedureName, int initiatorHostId, int partition)
	{
		m_procedureName = procedureName;
		m_initiatorHostId = initiatorHostId;
		m_partition = partition;
		m_isSinglePartition = true;
		m_hashCode = calcHashCode();
	}

	/**
	 * Reset the key to be for a group of multi-(every)-partition transactions
	 * with the given stored procedure, initiator host id.
	 */
	public void reset(String procedureName, int initiatorHostId)
	{
		m_procedureName = procedureName;
		m_initiatorHostId = initiatorHostId;
		m_partition = 0;
		m_isSinglePartition = false;
		m_hashCode = calcHashCode();
	}

	public String getProcedureName() {
		return m_procedureName;
	}

	public int getInitiatorHostId() {
		return m_initiatorHostId;
	}

	public boolean isSinglePartition() {
		return m_isSinglePartition;
	}

	public int getPartition() {
		return m_partition;
	}

	public boolean isMultiPartition() {
		return !isSinglePartition();
	}

	public int calcHashCode() {
		int hash = 0;
		int prime = 23;

		hash = m_procedureName.hashCode();
		hash = prime * hash + m_initiatorHostId;
		hash = prime * hash + (m_isSinglePartition ? 0 : 1);
		if (m_isSinglePartition) {
			hash = prime * hash + m_partition;
		}

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

		if (key.m_isSinglePartition != m_isSinglePartition) {
			return false;
		}

		if (m_isSinglePartition && key.m_partition != m_partition) {
			return false;
		}

		return true;
	}
}
