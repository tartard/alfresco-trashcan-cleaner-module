/*
 * #%L
 * Alfresco Trash Can Cleaner
 * %%
 * Copyright (C) 2005 - 2016 Alfresco Software Limited
 * %%
 * This file is part of the Alfresco software.
 * If the software was purchased under a paid Alfresco license, the terms of
 * the paid license agreement will prevail.  Otherwise, the software is
 * provided under the following open source license terms:
 *
 * Alfresco is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Alfresco is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Alfresco. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
package org.alfresco.trashcan;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.repo.transaction.RetryingTransactionHelper.RetryingTransactionCallback;
import org.alfresco.service.cmr.repository.ChildAssociationRef;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.service.transaction.TransactionService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * This class is capable of cleaning the trashcan without depending on searches
 * over the archive store. So the corresponding index core could be deactivated
 * with no impact on its execution. It will {@link #clean} the trashcan according
 * to defined {@link #deleteBatchCount} and {@link #daysToKeep} properties.
 * <p>
 * {@link #deleteBatchCount}: It will set how many nodes in trashcan to delete at
 * maximum during {@link #clean} execution. By default the value is 1000.
 * <p>
 * {@link #daysToKeep}: The max age in days for which documents in
 * trashcan are kept. Any nodes archived less than the value specified won't be deleted during {@link #clean} execution.
 *
 * @author Rui Fernandes
 * @author sglover
 */
public class TrashcanCleaner
{
    private static final Log logger = LogFactory.getLog(TrashcanCleaner.class);

	private static final long DAYS_TO_MILLIS = 1000 * 60 * 60 * 24;

    private final NodeService nodeService;
    private final TransactionService transactionService;

    private final String archiveStoreUrl = "archive://SpacesStore";
    private final int deleteBatchCount;
    private final int daysToKeep;
    private List<NodeRef> trashcanNodes;


    /**
	 *
	 *
	 * @param nodeService
     * @param transactionService
	 * @param deleteBatchCount
     * @param daysToKeep
	 */
    public TrashcanCleaner(NodeService nodeService, TransactionService transactionService,
            int deleteBatchCount, int daysToKeep)
	{
        this.nodeService = nodeService;
        this.transactionService = transactionService;
		this.deleteBatchCount = deleteBatchCount;
		this.daysToKeep = daysToKeep;
	}

    /**
     *
     * It deletes the {@link java.util.List List} of
     * {@link org.alfresco.service.cmr.repository.NodeRef NodeRef} received as
     * argument.
     *
     * @param nodes
     */
    private void deleteNodes(List<NodeRef> nodes)
    {
        for (final NodeRef nodeRef : nodes)
        {
            // create a new transaction for each deletion so the transactions are smaller and the progress of the
            // cleaner is not lost in case of any problems encountered during the job execution
            AuthenticationUtil.runAsSystem(new AuthenticationUtil.RunAsWork<Void>()
            {
                @Override
                public Void doWork() throws Exception {
                    RetryingTransactionCallback<Void> txnWork = new RetryingTransactionCallback<Void>()
                    {
                        @Override
                        public Void execute() throws Throwable
                        {
                            nodeService.deleteNode(nodeRef);
                            return null;
                        }
                    };
                    return transactionService.getRetryingTransactionHelper().doInTransaction(txnWork, false, true);
                }
            });
        }
    }

    /**
     * 
     * It returns the {@link java.util.List List} of
     * {@link org.alfresco.service.cmr.repository.NodeRef NodeRef} of the
     * archive store set to be deleted according to configuration for
     * <b>deleteBatchCount</b> and <b>daysToKeep</b>.
     * 
     * @return
     */
    private List<NodeRef> getBatchToDelete()
    {
        List<ChildAssociationRef> childAssocs = getTrashcanChildAssocs();
        List<NodeRef> nodes = new ArrayList<>(deleteBatchCount);
        if (logger.isDebugEnabled())
        {
            logger.debug(String.format("Found %s nodes on trashcan", childAssocs.size()));
        }
        return fillBatchToDelete(nodes, childAssocs);
    }

    /**
     *
     * It will fill up a {@link java.util.List List} of
     * {@link org.alfresco.service.cmr.repository.NodeRef NodeRef} from all the
     * {@link org.alfresco.service.cmr.repository.ChildAssociationRef
     * ChildAssociationRef} of the archive store set, according to the limit
     * parameters: <b>deleteBatchCount</b> and <b>daysToKeep</b>.
     *
     * @param batch
     * @param trashChildAssocs
     * @return
     */
    private List<NodeRef> fillBatchToDelete(List<NodeRef> batch, List<ChildAssociationRef> trashChildAssocs)
    {
        for (int j = trashChildAssocs.size(); j > 0 && batch.size() < deleteBatchCount; j--)
        {
            ChildAssociationRef childAssoc = trashChildAssocs.get(j - 1);
            NodeRef childRef = childAssoc.getChildRef();
            if (olderThanDaysToKeep(childRef))
            {
                batch.add(childRef);
            }
        }
        return batch;
    }

    /**
     *
     * It will return the first {@link #deleteBatchCount}
     * {@link org.alfresco.service.cmr.repository.ChildAssociationRef}s
     * of type {@link ContentModel}.ASSOC_CHILDREN
     * from the archive store set.
     *
     * @return
     */
	private List<ChildAssociationRef> getTrashcanChildAssocs()
	{
		StoreRef archiveStore = new StoreRef(archiveStoreUrl);
		NodeRef archiveRoot = nodeService.getRootNode(archiveStore);
        return nodeService.getChildAssocs(archiveRoot);
    }

	/**
	 *
	 * It checks if the archived node has been archived since longer than
     * <b>daysToKeep</b>. If <b>daysToKeep</b> is 0 or negative it will return
	 * always true.
	 *
	 * @param node
	 * @return
	 */
	private boolean olderThanDaysToKeep(NodeRef node)
	{
		if (daysToKeep <= 0)
			return true;
		Date archivedDate = (Date) nodeService.getProperty(node,
		        ContentModel.PROP_ARCHIVED_DATE);
		long archivedDateValue=0;
		if(archivedDate!=null)
			archivedDateValue=archivedDate.getTime();
		return daysToKeep * DAYS_TO_MILLIS < System.currentTimeMillis()
		        - archivedDateValue;
	}


    /**
     *
     * It returns the number of nodes present on trashcan.
     *
     * @return
     */
    public long getNumberOfNodesInTrashcan()
    {
        return getTrashcanChildAssocs().size();
    }

    /**
     * 
     * The method that will clean the specified <b>archiveStoreUrl</b> to the
     * limits defined by the values set for <b>deleteBatchCount</b> and
     * <b>daysToKeep</b>.
     * 
     */
    public void clean()
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Running TrashcanCleaner");
        }

        // Retrieve in a new read-only transaction the list of nodes to be deleted by the Trashcan Cleaner
        AuthenticationUtil.runAsSystem(new AuthenticationUtil.RunAsWork<Void>()
        {
            @Override
            public Void doWork() throws Exception {
                RetryingTransactionCallback<Void> txnWork = new RetryingTransactionCallback<Void>() {
                    @Override
                    public Void execute() throws Throwable {
                        trashcanNodes = getBatchToDelete();

                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("Number of nodes to delete: %s", trashcanNodes.size()));
                        }

                        return null;
                    }
                };
                return transactionService.getRetryingTransactionHelper().doInTransaction(txnWork, true, true);
            }
        });

        deleteNodes(trashcanNodes);

        if (logger.isDebugEnabled())
        {
            logger.debug("TrashcanCleaner finished");
        }
    }
}
