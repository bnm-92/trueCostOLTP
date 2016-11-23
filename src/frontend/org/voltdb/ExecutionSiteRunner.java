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

package org.voltdb;

import java.util.HashSet;

import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.Mailbox;

/**
 * A class that instantiates an ExecutionSite and then waits for notification before
 * running the execution site. Would it be better if this extended Thread
 * so we don't have to have m_runners and m_siteThreads?
 */
public class ExecutionSiteRunner implements Runnable {

    volatile boolean m_isSiteCreated = false;
    final int m_siteId;
    private final String m_serializedCatalog;
    volatile ExecutionSite m_siteObj;
    private final boolean m_recovering;
    private final boolean m_replicationActive;
    private final HashSet<Integer> m_failedHostIds;
    private final long m_txnId;
    private boolean wait = false;
    private int srcSite = -1;
    
    
    public ExecutionSiteRunner(
            final int siteId,
            final CatalogContext context,
            final String serializedCatalog,
            boolean recovering,
            boolean replicationActive,
            HashSet<Integer> failedHostIds,
            VoltLogger hostLog) {
        m_siteId = siteId;
        m_serializedCatalog = serializedCatalog;
        m_recovering = recovering;
        m_replicationActive = replicationActive;
        m_failedHostIds = failedHostIds;
        m_txnId = context.m_transactionId;
    }

    
    public ExecutionSiteRunner(
            final int siteId,
            final CatalogContext context,
            final String serializedCatalog,
            boolean recovering,
            boolean replicationActive,
            HashSet<Integer> failedHostIds,
            VoltLogger hostLog, boolean wait, int srcSite) {
        m_siteId = siteId;
        m_serializedCatalog = serializedCatalog;
        m_recovering = recovering;
        m_replicationActive = replicationActive;
        m_failedHostIds = failedHostIds;
        m_txnId = context.m_transactionId;
        this.wait = wait;
        this.srcSite = srcSite;
    }
    
    @Override
    public void run() {
//    	System.out.println("running execution site runner");
        Mailbox mailbox = VoltDB.instance().getMessenger()
        .createMailbox(m_siteId, VoltDB.DTXN_MAILBOX_ID, true);

        m_siteObj =
            new ExecutionSite(VoltDB.instance(),
                              mailbox,
                              m_siteId,
                              m_serializedCatalog,
                              null,
                              m_recovering,
                              m_replicationActive,
                              m_failedHostIds,
                              m_txnId);
        
        if (this.srcSite != -1) {
        	m_siteObj.sourceSite = this.srcSite; 
        }
//        System.out.println("enter synch block");
        
        if (!wait) {
        	synchronized (this) {
                m_isSiteCreated = true;
                this.notifyAll();
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        
//        System.out.println("enter try");
        try
        {
//        	System.out.println("starting sites");
            m_siteObj.run();
        }
        catch (OutOfMemoryError e)
        {
            // Even though OOM should be caught by the Throwable section below,
            // it sadly needs to be handled seperately. The goal here is to make
            // sure VoltDB crashes.

            String errmsg = "ExecutionSite: " + m_siteId + " ran out of Java memory. " +
                "This node will shut down.";
            VoltDB.crashLocalVoltDB(errmsg, true, e);
        }
        catch (Throwable t)
        {
            String errmsg = "ExecutionSite: " + m_siteId + " encountered an " +
                "unexpected error and will die, taking this VoltDB node down.";
            VoltDB.crashLocalVoltDB(errmsg, true, t);
        }
    }

}