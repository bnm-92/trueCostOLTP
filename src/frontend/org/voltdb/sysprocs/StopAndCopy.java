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
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.messaging.HostMessenger;

@ProcInfo(singlePartition = false)
public class StopAndCopy extends VoltSystemProcedure {

    private static final int DEP_stcAllNodeWork = (int)
            SysProcFragmentId.PF_stcDeclare | DtxnConstants.MULTIPARTITION_DEPENDENCY;

        private static final int DEP_stcAggregate = (int)
            SysProcFragmentId.PF_stcAggregate;

    //Turn on some sleeps to assist in creating interesting failures
    public static boolean debugFlag = false;

    @Override
    public void init() {
    	registerPlanFragment(SysProcFragmentId.PF_stcBlock);
    	registerPlanFragment(SysProcFragmentId.PF_stcAggregate);
    	registerPlanFragment(SysProcFragmentId.PF_stcDeclare);
//        registerPlanFragment(SysProcFragmentId.PF_stcCommit);
//        registerPlanFragment(SysProcFragmentId.PF_stcRollback);
    }

    VoltTable phaseZeroBlock(int hostId)
    {
        VoltTable retval = new VoltTable(
                new ColumnInfo("HostId", VoltType.INTEGER),
                new ColumnInfo("Error", VoltType.STRING) // string on failure, null on success
        );

        retval.addRow(hostId, null);
        return retval;
    }



    VoltTable aggregateResults(List<VoltTable> dependencies) {
        VoltTable retval = new VoltTable(
                new ColumnInfo("HostId", VoltType.INTEGER),
                new ColumnInfo("Error", VoltType.STRING) // string on failure, null on success
        );

        // take all the one-row result tables and collect any errors
        for (VoltTable table : dependencies) {
            table.advanceRow();
            String errMsg = table.getString(1);
            int hostId = (int) table.getLong(0);
            if (errMsg != null) {
                retval.addRow(hostId, errMsg);
            }
        }

        return retval;
    }
    
    
    /**
    *
    * @param rejoiningHostname
    * @param portToConnect
    * @return
    */
   VoltTable stcDeclare(
           int srcHost,
           int srcSite,
           int destHost,
           int destSite, 
           SystemProcedureExecutionContext context) {
	   
	   // verify valid hostId
       // connect	   
       VoltTable retval = new VoltTable(
               new ColumnInfo("HostId", VoltType.INTEGER),
               new ColumnInfo("Error", VoltType.STRING) // string on failure, null on success
       );
       
       int hostId = Integer.parseInt(context.getSite().getTypeName());
       String error = null;
       
       if (destSite == -1) {
    	   error = "could not find a site to replace it with";
    	   retval.addRow(hostId, error);
    	   return retval;
       }
       
//       System.out.println("dest site is " + destSite);
       Cluster cluster = context.getCluster();
       Partition srcPartitions = null; 
       StringBuilder sb = new StringBuilder();
       String site_path = "";
       
       for (Site site : cluster.getSites()) {
    	   int siteId = Integer.parseInt(site.getTypeName());
    	   //for dest
    	   if (destSite == siteId) {
    		   sb.append("set ");
    		   site_path = VoltDB.instance().getCatalogContext().catalog.
                       	getClusters().get("cluster").getSites().
                       	get(Integer.toString(siteId)).getPath();
    		   			sb.append(site_path).append(" ").append("isUp true");
//    		   			sb.append(site_path).append(" ").append("isexec true");
    	   }    	   
    	   
    	   //for src, find out partitions
    	   else if (srcSite == siteId) {
    		   srcPartitions = site.getPartition();
    		   
//    		   System.out.println("got source partiton" + srcPartitions);
    	   } else {
    		   continue;
    	   }
       }
       
       
//       sb.append(site_path).append(" ").append("partition " + srcPartitions.toString());
       sb.append("\n");
       String catalogDiffCommands = sb.toString();
       
//       if (destSite == context.getExecutionSite().getSiteId()) {
//    	   context.getExecutionSite().sourceSite = srcSite;
//    	   context.getExecutionSite().m_recovering = true;
//       }
       
//       System.out.println("host id is: "+ VoltDB.instance().getHostMessenger().getHostId());
       error = VoltDB.instance().doSTCPrepare(srcHost, srcSite, 
    		   destHost, destSite, m_runner.getTxnState().txnId, catalogDiffCommands);
       context.getExecutionSite().updateClusterState(catalogDiffCommands);
//       if (VoltDB.instance().getHostMessenger().getHostId() == destHost) {
//    	   System.out.println("at destHost");
    	   
           
//       }
//        if (destSite == this.m_site.getSiteId()) {
//        	this.m_site.m_shouldContinue = true;
//        }
       
       retval.addRow(hostId,error);
       
       return retval;
   }
    
    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
//    	System.out.println("finding out depPair");
        HostMessenger messenger = (HostMessenger) VoltDB.instance().getMessenger();
        int hostId = messenger.getHostId();
        Object[] oparams = params.toArray();
//        System.out.println("length of oparams is: "+oparams.length);
        VoltTable depResult = null;

        if (fragmentId == SysProcFragmentId.PF_stcBlock) {
//        	System.out.println("here at " + hostId);
            depResult = phaseZeroBlock(hostId);
            return new DependencyPair(DEP_stcAllNodeWork, depResult);
        
        } 
        else if (fragmentId == SysProcFragmentId.PF_stcAggregate) {
//        	System.out.println("aggregate");
            depResult = aggregateResults(dependencies.get(DEP_stcAllNodeWork));
            return new DependencyPair(DEP_stcAggregate, depResult);
            
        }
        else if (fragmentId == SysProcFragmentId.PF_stcDeclare) {
        	
        	
        	
            int srcHost = new Integer((String)oparams[0]);
            int srcSite = new Integer((String)oparams[1]);
            int destHost = new Integer((String)oparams[2]);
            int destSite = new Integer((String)oparams[3]);
            
            
            depResult =
                stcDeclare(
                		srcHost,
                		srcSite,
                		destHost, 
                		destSite,
                		context);
            return new DependencyPair(DEP_stcAllNodeWork, depResult);
        }        
        else {
            // really shouldn't ever get here
            assert(false);
            return null;
        }
    }

	public VoltTable[] run(SystemProcedureExecutionContext ctx, String srcHost, String srcSite, String destHost, String destSite) {
        // start the chain of joining plan fragments

        int dSite = new Integer(destSite);
        int dHost = new Integer(destHost);

        VoltDBInterface instance = VoltDB.instance();
        SiteTracker st = instance.getCatalogContext().siteTracker;
       
        // find site that we need
        if (dSite == -1) {
           for (int siteId: st.getAllSitesForHost(dHost)) {
             Site site = st.getSiteForId(siteId);
              if (site.getIsexec() && !site.getIsup()) {
                 dSite = siteId;
                break;
              }
            }
        }
        
        if (dSite == -1) {
          System.out.println("Sys proc failed");
          return null;
        }

        destSite = String.valueOf(dSite);

        VoltTable[] results;
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];
        
        // create a work fragment to block the whole cluster so that no
        // long-tail execution site work can cause late failure (ENG-1051)
        
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_stcBlock;
        pfs[1].outputDepId = DEP_stcAllNodeWork;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
//        pfs[1].parameters.setParameters();

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_stcAggregate;
        pfs[0].outputDepId = DEP_stcAggregate;
        pfs[0].inputDepIds = new int[]{ DEP_stcAllNodeWork };
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
//        System.out.println("System blocked, now getting if it was successful");
        
        results = executeSysProcPlanFragments(pfs, DEP_stcAggregate);
        
      //check results for errors
//        System.out.println("system blocked");
        
        
        
        pfs = new SynthesizedPlanFragment[2];
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_stcDeclare;
        pfs[1].outputDepId = DEP_stcAllNodeWork;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
//        System.out.println("at site : " + ctx.getExecutionSite().getSiteId());
//        System.out.println("values sent:");
//        System.out.println(srcHost);
//        System.out.println(srcSite);
//        System.out.println(destHost);
//        System.out.println(destSite);
        Object[] params = {srcHost, srcSite, destHost, destSite};
        pfs[1].parameters.setParameters(params);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_stcAggregate;
        pfs[0].outputDepId = DEP_stcAggregate;
        pfs[0].inputDepIds = new int[]{ DEP_stcAllNodeWork };
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
//        System.out.println("STC Prepare executed");
        results = executeSysProcPlanFragments(pfs, DEP_stcAggregate);
        VoltTable errorTable = results[0];
        if (errorTable == null) {
        	System.out.println("aggregation table issues");
        }
        else if (errorTable.getRowCount() > 0) {
        	System.out.println("\n something bad happened, aggregate result is not null \n");
        } else {
        	System.out.println("STC decleration completed");
        }
        
        //check results for errors
        
        
        return results;
    }

}
