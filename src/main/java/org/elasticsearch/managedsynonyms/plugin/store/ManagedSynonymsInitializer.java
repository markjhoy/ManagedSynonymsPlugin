package org.elasticsearch.managedsynonyms.plugin.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.gateway.GatewayService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ManagedSynonymsInitializer implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(ManagedSynonymsInitializer.class);

    private boolean isMaster = false;
    private final Client client;
    private final AtomicBoolean isIndexCreationInProgress = new AtomicBoolean(false);

    public ManagedSynonymsInitializer(Client client) {
        this.client = client;
    }

    public void onMaster() {
        // TODO - setup thread to ping to sync nodes as needed
    }

    public void offMaster() {
        // TODO - stop thread to ping to sync nodes
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        logger.info("On clusterChanged");
        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
            if (this.isMaster) {
                onMaster();
            } else {
                offMaster();
            }
        }

        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // Wait until the gateway has recovered from disk.
            return;
        }

        // The atomic flag prevents multiple simultaneous attempts to create the
        // index if there is a flurry of cluster state updates in quick succession
        if (this.isMaster && isIndexCreationInProgress.compareAndSet(false, true)) {
            try {
                logger.info("Creating managed synonyms index");
                ManagedSynonymStore.getInstance()
                    .syncFromIndex(
                        this.client,
                        0,
                        event.state(),
                        MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
                        ActionListener.wrap(r -> isIndexCreationInProgress.set(false), e -> {
                            isIndexCreationInProgress.set(false);
                            logger.error("Could not sync managed synonyms from index", e);
                        })
                    );
            } catch (IOException e) {
                // TODO Auto-generated catch block
                logger.error("Could not sync managed synonyms from index", e);
            }
        }
    }

}
