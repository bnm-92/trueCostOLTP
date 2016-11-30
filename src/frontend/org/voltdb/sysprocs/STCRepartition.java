/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Site;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.repartitioner.*;


@ProcInfo(singlePartition = false)
public class STCRepartition extends VoltSystemProcedure {
	WorkloadSampleStats wst;
	PartitioningGenerator gen;
	
	private static final int DEP_repartitionAllNodeWork = (int)
            SysProcFragmentId.PF_repartitionInfo | DtxnConstants.MULTIPARTITION_DEPENDENCY;

        private static final int DEP_repartitionAggregate = (int)
            SysProcFragmentId.PF_repartitionInfoAggregate;
	
    @Override
    public void init() {
    }

    VoltTable aggregateResults(List<VoltTable> dependencies) {
//        VoltTable retval = new VoltTable(
//        		new ColumnInfo("SiteId", VoltType.INTEGER),
//                new ColumnInfo("TxnId", VoltType.BIGINT),
//                new ColumnInfo("startTime", VoltType.BIGINT),
//                new ColumnInfo("endTime", VoltType.BIGINT),
//                new ColumnInfo("StoredProcedure", VoltType.STRING),
//                new ColumnInfo("partitions", VoltType.STRING),
//                new ColumnInfo("Error", VoltType.STRING) // string on failure, null on success
//        );
//
//        // take all the one-row result tables and collect any errors
//        for (VoltTable table : dependencies) {
//            while (table.advanceRow()) {
//            	int siteId = (int) table.get("SiteId", VoltType.INTEGER);
//            	long txnId = (long) table.get("TxnId", VoltType.BIGINT);
//            	long startTime = (long) table.get("startTime", VoltType.BIGINT);
//            	long endTime = (long) table.get("EndTime", VoltType.BIGINT);
//            	boolean check = false;
//	            String storedProc = (String) table.get("StoredProcedure", VoltType.STRING);
//	            String partitions = (String) table.get("partitions", VoltType.STRING);
//            	if (siteId == 0 || siteId == 100) {
//            		// adding hardcoded values for initiator values
//            		check = true;
//            	}
//            	if (check) {
//            		retval.addRow(siteId, txnId, startTime, endTime, storedProc, partitions);
//            	} else {
//            		
//            	}
//            }
//        	table.advanceRow();
//            String aggMsg = table.getString(1);
//            int hostId = (int) table.getLong(0);
////            if (errMsg != null) {
////                retval.addRow(hostId, errMsg);
////            }
//        }
//
        return null;
    }
    
    VoltTable collectStats() {
    	
    	return null;	
    }
    
    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command) {
    	VoltTable[] results;
    	
    	if (command.equals("init")) {
    		wst = new WorkloadSampleStats();
        	int[] allHost = {0,1};
        	int[] allPartitions = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
        	gen = new PartitioningGenerator(allHost, allPartitions, 8);
        	
        	
        	
    	} else if (command.equals("buildStats")) {
    		// foreach single partition
    		// recordSinglePa
    		
    		// foreach multi partion
    		// addMultiPartitionTransaction
    		// foreach partition p that the txn uses
    		//   if remote latency is non-zero
    		//      recordMultiPartitionTransactionRemotePartitionNetworkLatency
    		
    	} else if (command.equals("repartition")) {
    		
    	}
    	
    	return null;
    }


	@Override
	public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
			ParameterSet params, SystemProcedureExecutionContext context) {
		// TODO Auto-generated method stub
		 	HostMessenger messenger = (HostMessenger) VoltDB.instance().getMessenger();
	        int hostId = messenger.getHostId();
	        Object[] oparams = params.toArray();
//	        System.out.println("length of oparams is: "+oparams.length);
	        VoltTable depResult = null;
	        
	        if (fragmentId == SysProcFragmentId.PF_repartitionInfoAggregate) {
	        	depResult = aggregateResults(dependencies.get(this.DEP_repartitionAllNodeWork));
	            return new DependencyPair(DEP_repartitionAggregate, depResult);
	        } else if (fragmentId == SysProcFragmentId.PF_repartitionInfo) {
	        	depResult = collectStats();
	            return new DependencyPair(DEP_repartitionAllNodeWork, depResult);

	        }
		
		
		return null;
	}
}
