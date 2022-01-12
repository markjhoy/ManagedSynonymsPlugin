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
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymStore;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ManagedSynonymsPostAction extends ManagedSynonymsBaseAction {
    private static final Logger logger = LogManager.getLogger(ManagedSynonymsPostAction.class);

    ManagedSynonymsPostAction(Environment env) {
        super(env);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_synonyms/{filter}"));
    }

    @Override
    public String getName() {
        return "managed_synonyms_post_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        var filterName = restRequest.param("filter");
        var synonyms = ManagedSynonymStore.getInstance().getSynonymFile(filterName);
        if (synonyms == null) return returnErrorResponse(Collections.emptyList(), restRequest, client, RestStatus.NOT_FOUND);

        var content = contentAsMap(restRequest.content(), restRequest.getXContentType());
        if (content.size() == 0 || content.containsKey("synonyms") == false) {
            logger.error("Could not get synonyms content. Key was missing or empty");
            return returnErrorResponse(
                Collections.singletonList("missing body parameter 'synonyms'"),
                restRequest,
                client,
                RestStatus.BAD_REQUEST
            );
        }

        var contentSynonymsArray = content.get("synonyms");
        if ((contentSynonymsArray instanceof ArrayList) == false) {
            logger.error("Could not get synonyms content. Content type was: " + contentSynonymsArray.getClass().getName());
            return returnErrorResponse(
                Collections.singletonList("missing body parameter 'synonyms'"),
                restRequest,
                client,
                RestStatus.BAD_REQUEST
            );
        }

        try {
            @SuppressWarnings("unchecked")
            var createdSet = synonyms.createSynonymSet((ArrayList<String>) contentSynonymsArray);
            var response = new SynonymSetItemResponse(createdSet);
            return returnResponse(response, restRequest, client);
        } catch (ManagedSynonymException ex) {
            return returnErrorResponse(Collections.singletonList(ex.getMessage()), restRequest, client, RestStatus.BAD_REQUEST);
        }
    }
}
