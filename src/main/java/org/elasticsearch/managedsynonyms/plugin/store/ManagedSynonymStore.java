package org.elasticsearch.managedsynonyms.plugin.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.managedsynonyms.plugin.ManagedSynonymException;
import org.elasticsearch.managedsynonyms.plugin.ManagedSynonymTokenHelper;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class ManagedSynonymStore {
    private static final Logger logger = LogManager.getLogger(ManagedSynonymStore.class);

    private static ManagedSynonymStore instance;
    public static final String SYNONYM_INDEX = ".managed_synonyms_store";
    public static final String INDEX_DESCRIPTION = "Storage for managed synonyms plugin";
    public static final String VERSION_IDENTIFIER = "1.0.0";

    private static Map<String, ManagedSynonymFile> synonymFiles = new HashMap<String, ManagedSynonymFile>();

    public static ManagedSynonymStore getInstance() {
        if (instance == null) {
            logger.info("New instance created");
            instance = new ManagedSynonymStore();
        }
        return instance;
    }

    public ManagedSynonymFile getSynonymFile(String name) {
        if (synonymFiles.containsKey(name)) {
            return synonymFiles.get(name);
        }
        return null;
    }

    public ManagedSynonymFile getOrCreateSynonymFile(String name) {
        if (synonymFiles.containsKey(name)) {
            return synonymFiles.get(name);
        }

        var newFile = new ManagedSynonymFile(name);
        synonymFiles.put(name, new ManagedSynonymFile(name));
        return newFile;
    }

    public void clear() {
        synonymFiles.clear();
    }

    public void syncToIndex(Client client) {

    }

    /**
     * Refreshes the store with any newly updated items since "since"
     * @param client the Elasticsearch client
     * @param since minimum timestamp for filtering. Use 0L to sync all items
     * @return the time stamp of the most recently updated item
     * @throws IOException 
     */
    public long syncFromIndex(
        Client client,
        long since,
        ClusterState state,
        TimeValue masterNodeTimeout,
        final ActionListener<Boolean> finalListener
    ) throws IOException {
        if (doesManagedSynonymsIndexExist(state) == false) {
            // create our index
            try {
                createManagedSynonymsIndex(client, state, masterNodeTimeout, finalListener);
            } catch (ManagedSynonymException e) {

            }
        }

        // clear the local file store just in case
        this.clear();

        // load the synonyms from the index

        // build the query
        /*
         * The query must have a large result size,
         * and filter on since > updatedTimestamp
         * order by synonym file name so we can push things up in order...
         * To load all, set since = 0
        // load items
        var mustQuery = new BoolQueryBuilder();
        
        // mustQuery.must(null);
        var requestBuilder = new SearchSourceBuilder();
        var request = new SearchRequest(new String[] { SYNONYM_INDEX }, requestBuilder);
        request.indices(SYNONYM_INDEX);
        var response = client.search(request).get();
        // sync files
         */

        return 0;
    }

    private boolean doesManagedSynonymsIndexExist(ClusterState state) {
        var indexLookup = state.getMetadata().getIndicesLookup();
        return (indexLookup.isEmpty() == false && indexLookup.containsKey(SYNONYM_INDEX) == true);
    }

    private void createManagedSynonymsIndex(
        Client client,
        ClusterState state,
        TimeValue masterNodeTimeout,
        ActionListener<Boolean> finalListener
    ) throws ManagedSynonymException {
        logger.info("Managed synonyms index does not exist... creating...");

        try {
            var propertiesMap = getSynonymIndexFieldMapping();
            var mappingsMap = Map.ofEntries(new AbstractMap.SimpleEntry<String, Object>("properties", propertiesMap));
            var createIndexBuilder = client.admin().indices().prepareCreate(SYNONYM_INDEX);
            createIndexBuilder.setMapping(mappingsMap);
            createIndexBuilder.setSettings(managedSynonymsIndexSettings());

            var response = createIndexBuilder.get();
            if (!response.isAcknowledged()) {
                var errorMessage = ManagedSynonymTokenHelper.getErrorMessageFromResponse(response);
                throw new ManagedSynonymException("Could not create managed synonyms index: " + errorMessage);
            }

        } catch (ResourceAlreadyExistsException e) {
            // already good - just return
        }
    }

    public static Settings managedSynonymsIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
            .build();
    }

    public static String managedSynonymsIndexMapping() throws IOException {
        var propertiesMap = getSynonymIndexFieldMapping();
        var indexMappingDefinition = Map.ofEntries(
            new AbstractMap.SimpleEntry<String, Object>(
                "_doc",
                Map.ofEntries(
                    new AbstractMap.SimpleEntry<String, Object>(
                        "_meta",
                        Map.ofEntries(new AbstractMap.SimpleEntry<String, Object>("version", VERSION_IDENTIFIER))
                    ),
                    new AbstractMap.SimpleEntry<String, Object>("properties", propertiesMap)
                )
            )
        );
        return ManagedSynonymTokenHelper.getMapAsJsonString(indexMappingDefinition);
    }

    /**
     * Creates the mapping properties for the synonyms index creation.
     * The index mapping is:
     * 
     * - synonymFile: string
     * - setId: string (uuid)
     * - createdTimestamp: long
     * - updatedTimestamp: long
     * - synonyms: [string, string, ...]
     *  
     * @return index mapping properties
     */
    private static Map<String, Object> getSynonymIndexFieldMapping() {
        var keywordFieldMap = Map.ofEntries(
            new AbstractMap.SimpleEntry<String, Object>("type", "keyword"),
            new AbstractMap.SimpleEntry<String, Object>("ignore_above", 256)
        );
        var keywordFieldPropertyMap = Collections.singletonMap("keyword", keywordFieldMap);
        var textTypeMap = Map.ofEntries(
            new AbstractMap.SimpleEntry<String, Object>("type", "text"),
            new AbstractMap.SimpleEntry<String, Object>("fields", keywordFieldPropertyMap)
        );
        var timestampTypeMap = Collections.singletonMap("type", "long");

        return Map.ofEntries(
            new AbstractMap.SimpleEntry<String, Object>("synonymFile", keywordFieldMap),
            new AbstractMap.SimpleEntry<String, Object>("setId", keywordFieldMap),
            new AbstractMap.SimpleEntry<String, Object>("createdTimestamp", timestampTypeMap),
            new AbstractMap.SimpleEntry<String, Object>("updatedTimestamp", timestampTypeMap),
            new AbstractMap.SimpleEntry<String, Object>("synonyms", textTypeMap)
        );
    }
}
