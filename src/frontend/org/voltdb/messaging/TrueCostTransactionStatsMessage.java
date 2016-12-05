package org.voltdb.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.dtxn.TrueCostTransactionStats;
import org.voltdb.utils.DBBPool;

public class TrueCostTransactionStatsMessage extends VoltMessage {
	private ArrayList<TrueCostTransactionStats> m_txnStatsList;
	
	public TrueCostTransactionStatsMessage(ArrayList<TrueCostTransactionStats> txnStatsList)
	{
		m_txnStatsList = txnStatsList;
	}

	@Override
	protected void initFromBuffer() {
		
	}

	@Override
	protected void flattenToBuffer(DBBPool pool) throws IOException {
		Map<String, Integer> procedureNames = new HashMap<String, Integer>();
	}
}
