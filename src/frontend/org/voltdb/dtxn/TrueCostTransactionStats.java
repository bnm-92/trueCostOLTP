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
	
	private Long m_txnId = (long) -1;
	private String m_procedureName = null;
	private boolean m_singlePartition;
	private int m_partition = -1;
	private int m_initiatorHostId = -1;
	private int m_initiatorSiteId = -1;
	private int m_coordinatorSiteId = -1;
	private long m_latency = -1;
	private long m_startTime = -1;
	private long m_endTime = -1;
	

	public void setStartTime(long time) {
		this.m_startTime = time;
	}
	
	public void setEndTime(long time) {
		this.m_endTime = time;
	}
	
	// check this method later
	public void calculateLatency() {
		if ( (m_startTime != -1) && (m_endTime != -1) ) {
			m_latency = m_endTime - m_startTime;
		}
	}
	
	public TrueCostTransactionStats(Long txnId)
	{
		m_txnId = txnId;
	}
	
	public Long getTxnId() {
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
	
	public void setIsSinglePartition(boolean in, int partition) {
		this.m_singlePartition = in;
		if (partition != -1)
			this.m_partition = partition;
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

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (m_txnId != null) {
			sb.append("The transactionId is : ");
			sb.append(m_txnId);
			sb.append(" ");
		}
		if (m_procedureName != null) {
			sb.append("The procedure invocation is : ");
			sb.append(m_procedureName);
			sb.append(" ");
		}
		if (m_singlePartition) {
			sb.append("The single partition : ");
			sb.append(m_partition);
			sb.append(" ");
		} else {
			sb.append("Had All Partitions: ");
//			sb.append(m_procedureName);
			sb.append(" ");
		}
		if (m_initiatorSiteId != -1) {
			sb.append("The initiator Id is : ");
			sb.append(m_initiatorSiteId);
			sb.append(" ");
		}
		if (m_initiatorHostId != -1) {
			sb.append("The host Id is : ");
			sb.append(m_initiatorHostId);
			sb.append(" ");
		}
		if ( m_coordinatorSiteId != -1) {
			sb.append("The coordinator Id is : ");
			sb.append(m_coordinatorSiteId);
			sb.append(" ");
		}
		if ( m_latency != -1) {
			sb.append("The latency is : ");
			sb.append(m_latency);
			sb.append(" \n");
		}
		
		return sb.toString();
	}
	
}
