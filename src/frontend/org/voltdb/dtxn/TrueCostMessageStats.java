package org.voltdb.dtxn;

public class TrueCostMessageStats {
	private int m_senderSiteId;
	private int m_receiverSiteId;
	private long m_sendTime;
	private long m_receiveTime;
	
	/**
	 * 
	 * @param senderSiteId site id of site that sent the message.
	 * @param receiverSiteId site id of site that received the message.
	 * @param sendTime send time of the message.
	 * @param receiveTime receive time of the message.
	 */
	public TrueCostMessageStats(int senderSiteId, int receiverSiteId, long sendTime, long receiveTime)
	{
		m_senderSiteId = senderSiteId;
		m_receiverSiteId = receiverSiteId;
		m_sendTime = sendTime;
		m_receiveTime = receiveTime;
	}

	public int getSenderSiteId() {
		return m_senderSiteId;
	}

	public int getReceiverSiteId() {
		return m_receiverSiteId;
	}

	public long getSendTime() {
		return m_sendTime;
	}

	public long getReceiveTime() {
		return m_receiveTime;
	}
	
	public String toString() {
		
		
		return null;
	}
}
