/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.managedsynonyms.plugin.response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SynonymSetListResponse extends ActionResponse implements ToXContentObject {
    private static final Logger logger = LogManager.getLogger(SynonymSetListResponse.class);

    private List<ManagedSynonymSet> synonymSets;
    private int currentPage;
    private int pageSize;
    private int pageCount;
    private int totalCount;

    public SynonymSetListResponse(List<ManagedSynonymSet> resultSets, int currentPage, int pageSize, int pageCount, int totalCount) {
        this.synonymSets = new ArrayList<>(resultSets);
        this.currentPage = currentPage;
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        this.totalCount = totalCount;
    }

    SynonymSetListResponse(StreamInput in) {
        logger.info("in stream ctor");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        logger.info("in steam output");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        setOutputPageMeta(builder);
        builder.startArray("results");
        for (ManagedSynonymSet synonymSet : synonymSets) {
            synonymSet.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private void setOutputPageMeta(XContentBuilder builder) throws IOException {
        builder.startObject("meta")
            .startObject("page")
            .field("current", currentPage)
            .field("total_pages", pageCount)
            .field("total_results", totalCount)
            .field("size", pageSize)
            .endObject()
            .endObject();
    }

}
