/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.managedsynonyms.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.env.Environment;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymStore;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ManagedSynonymsDeleteAction extends ManagedSynonymsBaseAction {

    ManagedSynonymsDeleteAction(Environment env) {
        super(env);
    }

    @Override
    public String getName() {
        return "managed_synonyms_delete_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.DELETE, "/_synonyms/{filter}/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String filterName = request.param("filter");
        if (filterName == null || filterName.length() == 0) {
            return returnErrorResponse(Collections.emptyList(), request, client, RestStatus.NOT_FOUND);
        }

        var filterId = request.param("id", "");
        if (filterId.length() == 0) {
            return returnErrorResponse(Collections.emptyList(), request, client, RestStatus.NOT_FOUND);
        }

        var synonyms = ManagedSynonymStore.getInstance().getSynonymFile(filterName);
        if (synonyms == null) {
            return returnErrorResponse(Collections.emptyList(), request, client, RestStatus.NOT_FOUND);
        }

        if (synonyms.deleteSynonymSet(filterId) == false) {
            return returnErrorResponse(Collections.emptyList(), request, client, RestStatus.NOT_FOUND);
        }

        return channel -> {
            try {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, ""));
            } catch (final Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }

}
