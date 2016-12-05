package org.voltdb.dtxn;

import java.util.ArrayList;

import org.voltdb.VoltDB;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.SiteMailbox;
import org.voltdb.messaging.TrueCostTransactionStatsMessage;

public class TrueCostTransactionStatsSender extends Thread {

	/**
	 * Interval between sending each message.
	 */
	private static final int SEND_INTERVAL_MS = 100;

	/**
	 * Maximum number of transaction stats collected before we send/drain.
	 */
	private static final int MAX_TRANSACTION_STATS = 1000;

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
		while (true) {
			long sendTime = System.currentTimeMillis() + SEND_INTERVAL_MS;

			while (System.currentTimeMillis() < sendTime) {
				if (m_initiator.getTxnStatsList().size() >= MAX_TRANSACTION_STATS) {
					break;
				}

				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					return;
				}
			}

			m_initiator.swapTxnStatsList(m_txnStatsList);
			try {
				m_mailbox.send(0, VoltDB.COLLECTOR_MAILBOX_ID, new TrueCostTransactionStatsMessage(m_txnStatsList));
			} catch(MessagingException e) {
				e.printStackTrace();
			}
			m_txnStatsList.clear();
		}
	}

}
