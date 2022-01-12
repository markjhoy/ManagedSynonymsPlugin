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
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymSet;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymStore;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ManagedSynonymsUpdateAction extends ManagedSynonymsBaseAction {
    private static final Logger logger = LogManager.getLogger(ManagedSynonymsUpdateAction.class);

    ManagedSynonymsUpdateAction(Environment env) {
        super(env);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/_synonyms/{filter}/{id}"));
    }

    @Override
    public String getName() {
        return "managed_synonyms_update_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        var filterName = request.param("filter");
        var filterId = request.param("id", "");
        if (filterId.length() == 0) {
            return returnErrorResponse(request, client, RestStatus.NOT_FOUND);
        }

        var synonyms = ManagedSynonymStore.getInstance().getSynonymFile(filterName);
        if (synonyms == null) return returnErrorResponse(request, client, RestStatus.NOT_FOUND);

        var content = contentAsMap(request.content(), request.getXContentType());
        if (content.size() == 0 || content.containsKey("synonyms") == false) return returnErrorResponse(
            Collections.emptyList(),
            request,
            client,
            RestStatus.BAD_REQUEST
        );

        var contentSynonymsArray = content.get("synonyms");
        if ((contentSynonymsArray instanceof ArrayList) == false) {
            return returnErrorResponse(
                Collections.singletonList("missing body parameter 'synonyms'"),
                request,
                client,
                RestStatus.BAD_REQUEST
            );
        }

        var setToUpdate = new ManagedSynonymSet(filterId);

        @SuppressWarnings("unchecked")
        var synonymsList = (Collection<String>) contentSynonymsArray;
        setToUpdate.setList(synonymsList);

        try {
            var updatedSet = synonyms.updateSynonymSet(setToUpdate);
            if (updatedSet == null) {
                return returnErrorResponse(request, client, RestStatus.NOT_FOUND);
            }

            var response = new SynonymSetItemResponse(updatedSet);
            return returnResponse(response, request, client);
        } catch (ManagedSynonymException ex) {
            return returnErrorResponse(Collections.singletonList(ex.getMessage()), request, client, RestStatus.BAD_REQUEST);
        }

    }

}
