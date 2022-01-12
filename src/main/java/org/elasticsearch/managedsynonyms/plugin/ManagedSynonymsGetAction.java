/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.managedsynonyms.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.env.Environment;
import org.elasticsearch.managedsynonyms.plugin.response.SynonymSetItemResponse;
import org.elasticsearch.managedsynonyms.plugin.response.SynonymSetListResponse;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymStore;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ManagedSynonymsGetAction extends ManagedSynonymsBaseAction {
    private static final Logger logger = LogManager.getLogger(ManagedSynonymsPlugin.class);

    public ManagedSynonymsGetAction(Environment env) {
        super(env);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_synonyms/{filter}"),
            new Route(RestRequest.Method.GET, "/_synonyms/{filter}/{id}")
        );
    }

    @Override
    public String getName() {
        return "managed_synonyms_get_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String filterName = restRequest.param("filter");
        if (filterName == null || filterName.length() == 0) {
            return returnErrorResponse(Collections.emptyList(), restRequest, client, RestStatus.NOT_FOUND);
        }

        var filterId = restRequest.param("id", "");
        if (filterId.length() > 0) {
            return handleGetFilter(filterName, filterId, restRequest, client);
        }

        return handleGetFilterList(filterName, restRequest, client);
    }

    private RestChannelConsumer handleGetFilter(String filterName, String filterId, RestRequest restRequest, NodeClient client)
        throws IOException {
        var synonyms = ManagedSynonymStore.getInstance().getSynonymFile(filterName);
        if (synonyms == null) {
            return returnErrorResponse(restRequest, client, RestStatus.NOT_FOUND);
        }

        // TODO: if filterId == '_stats' return the stats

        var synonymSet = synonyms.getSet(filterId);
        if (synonymSet == null) {
            return returnErrorResponse(restRequest, client, RestStatus.NOT_FOUND);
        }
        var response = new SynonymSetItemResponse(synonymSet);
        return returnResponse(response, restRequest, client);
    }

    private RestChannelConsumer handleGetFilterList(String filterName, RestRequest restRequest, NodeClient client) throws IOException {
        var synonyms = ManagedSynonymStore.getInstance().getSynonymFile(filterName);
        if (synonyms == null) {
            return returnErrorResponse(restRequest, client, RestStatus.NOT_FOUND);
        }

        int currentPage = Integer.parseInt(restRequest.param("page", "1"));
        int pageSize = Integer.parseInt(restRequest.param("size", "25"));
        String queryFilter = restRequest.param("query", null);

        int totalCount = synonyms.getCount(queryFilter);
        var resultItems = synonyms.listSets(currentPage, pageSize, queryFilter);
        int pageCount = (totalCount / pageSize) + 1;

        var response = new SynonymSetListResponse(resultItems, currentPage, pageSize, pageCount, totalCount);
        return this.returnResponse(response, restRequest, client);
    }

}
