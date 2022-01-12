/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.managedsynonyms.plugin;

import static java.util.Collections.singletonMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.managedsynonyms.plugin.analyzer.ManagedSynonymTokenFilterFactory;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymStore;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymsInitializer;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ManagedSynonymsPlugin extends Plugin implements ActionPlugin, AnalysisPlugin, SystemIndexPlugin {
    private List<RestHandler> handlers = new ArrayList<RestHandler>();

    private static final Logger logger = LogManager.getLogger(ManagedSynonymsPlugin.class);
    public static final String MANAGED_SYNONYMS_ORIGIN = "ManagedSynonyms";
    private static ManagedSynonymsInitializer initializer;

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return singletonMap("managed_synonyms", ManagedSynonymTokenFilterFactory::new);
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        logger.info("Starting ManagedSynonyms Plugin");

        handlers.clear();
        handlers.addAll(
            Arrays.asList(
                new ManagedSynonymsGetAction(environment),
                new ManagedSynonymsPostAction(environment),
                new ManagedSynonymsUpdateAction(environment),
                new ManagedSynonymsDeleteAction(environment)
            )
        );

        initializer = new ManagedSynonymsInitializer(client);
        return Collections.singletonList(initializer);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        logger.info("Adding ManagedSynonymsPlugin REST handlers");
        return handlers;
    }

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
        // TODO - launch to ensure we have indices
        return Collections.emptyList();
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        try {
            return Collections.singletonList(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(ManagedSynonymStore.SYNONYM_INDEX + "*")
                    .setPrimaryIndex(ManagedSynonymStore.SYNONYM_INDEX)
                    .setSettings(ManagedSynonymStore.managedSynonymsIndexSettings())
                    .setMappings(ManagedSynonymStore.managedSynonymsIndexMapping())
                    .setVersionMetaKey("version")
                    .setDescription(ManagedSynonymStore.INDEX_DESCRIPTION)
                    .setOrigin(MANAGED_SYNONYMS_ORIGIN)
                    .build()
            );
        } catch (IOException e) {
            logger.error("IOException getting system index descriptors", e);
            return Collections.emptyList();
        }
    }

    @Override
    public String getFeatureName() {
        return "managed_synonyms";
    }

    @Override
    public String getFeatureDescription() {
        return "Provides REST managed synonym token filtering";
    }

    @Override
    public void close() throws IOException {
        flushSynonymStore();
    }

    private void flushSynonymStore() throws IOException {
        logger.info("Writing synonym store to cluster settings (not implemented currently)");
        // ManagedSynonymStore.getInstance().saveToSettings(this.clusterService);
    }

}
