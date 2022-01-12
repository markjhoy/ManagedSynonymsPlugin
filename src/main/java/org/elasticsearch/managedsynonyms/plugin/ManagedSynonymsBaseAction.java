/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.managedsynonyms.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.managedsynonyms.plugin.response.SynonymsErrorResponse;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class ManagedSynonymsBaseAction extends BaseRestHandler {
    protected final Environment environment;

    ManagedSynonymsBaseAction(Environment env) {
        super();
        this.environment = env;
    }

    protected RestChannelConsumer returnErrorResponse(RestRequest restRequest, NodeClient client, RestStatus status) {
        return returnErrorResponse(Collections.emptyList(), restRequest, client, status);
    }

    protected RestChannelConsumer returnErrorResponse(List<String> errors, RestRequest restRequest, NodeClient client, RestStatus status) {
        var response = new SynonymsErrorResponse(errors);
        return channel -> {
            try {
                var builder = channel.newBuilder();
                response.toXContent(builder, restRequest);
                channel.sendResponse(new BytesRestResponse(status, builder));
            } catch (final Exception ex) {
                channel.sendResponse(new BytesRestResponse(channel, ex));
            }
        };
    }

    protected RestChannelConsumer returnResponse(ToXContentObject responseItem, RestRequest restRequest, NodeClient client) {
        return channel -> {
            try {
                var builder = channel.newBuilder();
                responseItem.toXContent(builder, restRequest);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (final Exception ex) {
                channel.sendResponse(new BytesRestResponse(channel, ex));
            }
        };
    }

    protected Map<String, Object> contentAsMap(BytesReference content, XContentType contentType) {
        return XContentHelper.convertToMap(content, false, contentType).v2();
    }

}
