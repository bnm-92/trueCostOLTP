package org.voltdb;

import java.util.ArrayList;

public class stats {
	public long txnId;
	public String storedProcedure;
	public int hostId;
	public ArrayList<Integer> partitions;
	public Long initTime;
	public Long endTime;
	
	public stats () {
		
	}
}