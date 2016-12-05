package org.voltdb.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.dtxn.TrueCostTransactionStats;
import org.voltdb.utils.DBBPool;

public class TrueCostTransactionStatsMessage extends VoltMessage {
	private static final int TRUE_COST_TXN_STATS_SIZE = 8 + 4 + 1 + 4 + 4 + 4 + 4 + 8 + 8 + 8;
	
	private TrueCostTransactionStats[] m_txnStatsList;

	public TrueCostTransactionStatsMessage(ArrayList<TrueCostTransactionStats> txnStatsList) {
		m_txnStatsList = txnStatsList.toArray(new TrueCostTransactionStats[] {});
	}
	
	public TrueCostTransactionStats[] getTxnStatsList() {
		return m_txnStatsList;
	}

	@Override
	protected void initFromBuffer() {

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
			m_container = pool.acquire(4 + procedureNamesSize + 4 + m_txnStatsList.length * TRUE_COST_TXN_STATS_SIZE);
			m_buffer = m_container.b;
		}
		
		m_buffer.putInt(procedureNameIndex);
		for(Entry<String, Integer> procedureNamesEntry : procedureNames.entrySet()) {
			byte[] procedureNameBytes = procedureNamesEntry.getKey().getBytes();
			
			m_buffer.putInt(procedureNameBytes.length);
			m_buffer.put(procedureNameBytes);
		}
		
		m_buffer.putInt(m_txnStatsList.length);
		for(TrueCostTransactionStats txnStats : m_txnStatsList) {
			
		}
	}
}
