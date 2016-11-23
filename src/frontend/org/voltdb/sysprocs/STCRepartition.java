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

    
    @Override
    public void init() {
    }

    
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
    	VoltTable[] results;
    	    	
    	WorkloadSampleStats wst = new WorkloadSampleStats();
    	
    	
    	
    	return null;
    }


	@Override
	public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
			ParameterSet params, SystemProcedureExecutionContext context) {
		// TODO Auto-generated method stub
		return null;
	}
}
