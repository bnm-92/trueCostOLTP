package org.voltdb.dtxn;

/**
 * BABAR:
 * After instantiating call:
 * setPartitions()
 * setInitiatorHostId()
 * setInitiatorSiteId()
 * setCoordinatorSiteId()
 * 
 * When transaction done call:
 * setLatency()
 */
public class TrueCostTransactionStats {
	
	private int m_txnId;
	private String m_procedureName;
	private boolean m_singlePartition;
	private int m_partition;
	private int m_initiatorHostId;
	private int m_initiatorSiteId;
	private int m_coordinatorSiteId;
	private long m_latency;
	
	public TrueCostTransactionStats(int txnId)
	{
		m_txnId = txnId;
	}
	
	public int getTxnId() {
		return m_txnId;
	}

	public String getProcedureName() {
		return m_procedureName;
	}

	public void setProcedureName(String procedureName) {
		this.m_procedureName = procedureName;
	}

	public boolean isSinglePartition() {
		return m_singlePartition;
	}
	
	public boolean isEveryPartition() {
		return !isSinglePartition();
	}

	public void setPartitions(int[] partitions) {
		if(partitions.length > 1) {
			m_singlePartition = false;
		} else {
			assert(partitions.length == 1);
			m_partition = partitions[0];
			m_singlePartition = true;
		}
	}
	
	public int getPartition() {
		// Should not get this for every partition txn's
		assert(isSinglePartition());
		
		return m_partition;
	}

	public int getInitiatorHostId() {
		return m_initiatorHostId;
	}

	public void setInitiatorHostId(int initiatorHostId) {
		this.m_initiatorHostId = initiatorHostId;
	}

	public int getInitiatorSiteId() {
		return m_initiatorSiteId;
	}

	public void setInitiatorSiteId(int initiatorSiteId) {
		this.m_initiatorSiteId = initiatorSiteId;
	}

	public int getCoordinatorSiteId() {
		return m_coordinatorSiteId;
	}

	public void setCoordinatorSiteId(int coordinatorSiteId) {
		this.m_coordinatorSiteId = coordinatorSiteId;
	}

	public long getLatency() {
		return m_latency;
	}

	public void setLatency(long latency) {
		this.m_latency = latency;
	}
}
