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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SynonymsErrorResponse extends ActionResponse implements ToXContentObject {

    private final List<String> errors;

    public SynonymsErrorResponse() {
        this.errors = Collections.emptyList();
    }

    public SynonymsErrorResponse(List<String> errors) {
        this.errors = errors;
    }

    public SynonymsErrorResponse(StreamInput in) {
        this.errors = Collections.emptyList();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (this.errors == null || this.errors.size() == 0) return builder;

        builder.startObject();
        builder.array("errors", this.errors);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

}
