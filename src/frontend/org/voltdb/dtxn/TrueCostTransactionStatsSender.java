package org.voltdb.dtxn;

import java.util.ArrayList;

import org.voltdb.VoltDB;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.SiteMailbox;
import org.voltdb.messaging.TrueCostTransactionStatsMessage;

public class TrueCostTransactionStatsSender extends Thread {
	
	private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

	/**
	 * Interval between sending each message.
	 */
	private static final int SEND_INTERVAL_MS = 1;

	/**
	 * Maximum number of transaction stats collected before we send/drain.
	 */
	private static final int MAX_TRANSACTION_STATS = 100;

	private SimpleDtxnInitiator m_initiator;
	private SiteMailbox m_mailbox;
	private ArrayList<TrueCostTransactionStats> m_txnStatsList = new ArrayList<TrueCostTransactionStats>();

	public TrueCostTransactionStatsSender(SimpleDtxnInitiator initiator) {
		setDaemon(true);

		m_initiator = initiator;
		m_mailbox = (SiteMailbox) VoltDB.instance().getMessenger().createMailbox(m_initiator.getSiteId(),
				VoltDB.SENDER_MAILBOX_ID, false);
	}

	@Override
	public void run() {
		consoleLog.info("Started transaction stats sender thread for Host ID:" + m_initiator.getHostId() + " Site ID:" + m_initiator.getSiteId());
		
		while (true) {
			long sendTime = System.currentTimeMillis() + SEND_INTERVAL_MS;

			while (System.currentTimeMillis() < sendTime) {
				if (m_initiator.getNumTransactionStats() >= MAX_TRANSACTION_STATS) {
					break;
				}

				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					return;
				}
			}

			m_initiator.drainTransactionStats(m_txnStatsList);
			if(m_txnStatsList.size() > 0) {
				try {
					m_mailbox.send(0, VoltDB.COLLECTOR_MAILBOX_ID, new TrueCostTransactionStatsMessage(m_txnStatsList));
				} catch(MessagingException e) {
					e.printStackTrace();
				}
				m_txnStatsList.clear();
			}
		}
	}

}
