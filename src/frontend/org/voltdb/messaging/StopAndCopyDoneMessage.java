package org.voltdb.messaging;

import java.io.IOException;

import org.voltdb.utils.DBBPool;

public class StopAndCopyDoneMessage extends VoltMessage {
	
	public StopAndCopyDoneMessage() {}

	@Override
	protected void initFromBuffer() {
		// Nothing to do
	}

	@Override
	protected void flattenToBuffer(DBBPool pool) throws IOException {
		if(m_buffer == null) {
			m_container = pool.acquire(HEADER_SIZE + 1);
			m_buffer = m_container.b;
		}
		
		m_buffer.position(HEADER_SIZE);
		m_buffer.put(STOP_AND_COPY_DONE_ID);
	}

}
