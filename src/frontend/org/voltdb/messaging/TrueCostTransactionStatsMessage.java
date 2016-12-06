package org.voltdb.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.dtxn.TrueCostTransactionStats;
import org.voltdb.utils.DBBPool;

public class TrueCostTransactionStatsMessage extends VoltMessage {
	private static final int TRUE_COST_TXN_STATS_SIZE = 8 + 4 + 1 + 4 + 4 + 4 + 4 + 8;

	private TrueCostTransactionStats[] m_txnStatsList;
	
	public TrueCostTransactionStatsMessage() {}

	public TrueCostTransactionStatsMessage(ArrayList<TrueCostTransactionStats> txnStatsList) {
		m_txnStatsList = txnStatsList.toArray(new TrueCostTransactionStats[] {});
	}

	public TrueCostTransactionStats[] getTxnStatsList() {
		return m_txnStatsList;
	}

	@Override
	protected void initFromBuffer() {
		m_buffer.position(HEADER_SIZE + 1); // skip message id
		
		int procedureNamesSize = m_buffer.getInt();
		String[] procedureNames = new String[procedureNamesSize];

		for (int procedureNameIndex = 0; procedureNameIndex < procedureNamesSize; ++procedureNameIndex) {
			int procedureNameLength = m_buffer.getInt();
			byte[] procedureNameBytes = new byte[procedureNameLength];

			m_buffer.get(procedureNameBytes, 0, procedureNameLength);
			procedureNames[procedureNameIndex] = new String(procedureNameBytes);
		}

		int txnStatsListSize = m_buffer.getInt();
		m_txnStatsList = new TrueCostTransactionStats[txnStatsListSize];

		for (int i = 0; i < txnStatsListSize; i++) {
			TrueCostTransactionStats txnStats = new TrueCostTransactionStats(m_buffer.getLong());
			
			txnStats.setProcedureName(procedureNames[m_buffer.getInt()]);
			txnStats.setIsSinglePartition(m_buffer.get() == 1 ? true : false, m_buffer.getInt());
			txnStats.setInitiatorHostId(m_buffer.getInt());
			txnStats.setInitiatorSiteId(m_buffer.getInt());
			txnStats.setCoordinatorSiteId(m_buffer.getInt());
			txnStats.setLatency(m_buffer.getLong());
			
			m_txnStatsList[i] = txnStats;
		}
	}

	@Override
	protected void flattenToBuffer(DBBPool pool) throws IOException {
		Map<String, Integer> procedureNames = new HashMap<String, Integer>();
		int procedureNamesSize = 0;

		int procedureNameIndex = 0;
		for (TrueCostTransactionStats txnStats : m_txnStatsList) {
			String procedureName = txnStats.getProcedureName();

			if (!procedureNames.containsKey(procedureName)) {
				procedureNames.put(procedureName, procedureNameIndex);
				procedureNameIndex++;
				procedureNamesSize += 4 + procedureName.getBytes().length;
			}
		}

		if (m_buffer == null) {
			m_container = pool.acquire(HEADER_SIZE + 1 + 4 + procedureNamesSize + 4 + m_txnStatsList.length * TRUE_COST_TXN_STATS_SIZE);
			m_buffer = m_container.b;
		}
		
		m_buffer.position(HEADER_SIZE);
		m_buffer.put(TRUECOST_TXNSTATS_ID);

		m_buffer.putInt(procedureNameIndex);
		for (Entry<String, Integer> procedureNamesEntry : procedureNames.entrySet()) {
			byte[] procedureNameBytes = procedureNamesEntry.getKey().getBytes();

			m_buffer.putInt(procedureNameBytes.length);
			m_buffer.put(procedureNameBytes);
		}

		m_buffer.putInt(m_txnStatsList.length);
		for (TrueCostTransactionStats txnStats : m_txnStatsList) {
			m_buffer.putLong(txnStats.getTxnId());
			m_buffer.putInt(procedureNames.get(txnStats.getProcedureName()));
			m_buffer.put((byte) (txnStats.isSinglePartition() ? 1 : 0));
			m_buffer.putInt(txnStats.getPartition());
			m_buffer.putInt(txnStats.getInitiatorHostId());
			m_buffer.putInt(txnStats.getInitiatorSiteId());
			m_buffer.putInt(txnStats.getCoordinatorSiteId());
			m_buffer.putLong(txnStats.getLatency());
		}
		
		m_buffer.limit(m_buffer.position());
	}
}
