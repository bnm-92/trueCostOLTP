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
import org.voltdb.ExecutionSite;
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
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.messaging.HostMessenger;

@ProcInfo(singlePartition = false)
public class FailSite extends VoltSystemProcedure {

    //Turn on some sleeps to assist in creating interesting failures
    public static boolean debugFlag = false;

    @Override
    public void init() {
        
    }



    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command, String siteId) {
        // start the chain of joining plan fragments
    	
//    	System.out.println("in fail Site");
    	
//    	System.out.println(VoltDB.instance().getHostMessenger().getHostname());
    	
    	if (command.equals("update")) {
    		Site st = ctx.getSite();

//    		if (st.getIsup() && !st.getIsexec()) {
//    			System.out.println("reporting fault");
//    			System.out.println("reported fault at Site: " + Integer.parseInt(st.getTypeName()));
//        		NodeFailureFault fault = new NodeFailureFault(VoltDB.instance().getCatalogContext().siteTracker.
//        				getHostForSite(new Integer(siteId)),
//        				new Integer(siteId), true);
//        		VoltDB.instance().getFaultDistributor()
//        		.reportFault(fault);
//        	} else if (Integer.parseInt(st.getTypeName()) == 1){
//        		System.out.println("reported fault at Site: " + Integer.parseInt(st.getTypeName()));
        		NodeFailureFault fault = new NodeFailureFault(VoltDB.instance().getCatalogContext().siteTracker.
        				getHostForSite(new Integer(siteId)),
        				new Integer(siteId), true);
        		VoltDB.instance().getFaultDistributor()
        		.reportFault(fault);
//        	}
    	
    	} else {
    		StringBuilder sb = new StringBuilder();
    		for (Site s : ctx.getCluster().getSites()) {
        		
    			sb.append("\nSite: ");
    			sb.append(Integer.parseInt(s.getTypeName()));
    			sb.append(s.getPartition());
    			sb.append(s.getHost());
    			sb.append(" is up ");
    			sb.append(s.getIsup());
    			
        		
        	}
    		
    		String str = sb.toString();
    		System.out.println(str);
        	
        	
    	}
        
        return new VoltTable[0];
    }



	@Override
	public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
			ParameterSet params, SystemProcedureExecutionContext context) {
		// TODO Auto-generated method stub
		return null;
	}

}
