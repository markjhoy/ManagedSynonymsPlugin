/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.managedsynonyms.plugin.response;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymSet;

import java.io.IOException;

public class SynonymSetItemResponse extends ActionResponse implements ToXContentObject {
    private final ManagedSynonymSet synonymSet;

    public SynonymSetItemResponse(ManagedSynonymSet synonymSet) {
        this.synonymSet = synonymSet;
    }

    SynonymSetItemResponse(StreamInput in) {
        this.synonymSet = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(synonymSet.toString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        synonymSet.toXContent(builder, params);
        return builder;
    }
}
