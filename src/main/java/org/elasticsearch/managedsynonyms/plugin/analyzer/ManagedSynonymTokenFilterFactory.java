/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.managedsynonyms.plugin.analyzer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymSet;
import org.elasticsearch.managedsynonyms.plugin.store.ManagedSynonymStore;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.function.Function;

public class ManagedSynonymTokenFilterFactory extends AbstractTokenFilterFactory {
    private static final Logger logger = LogManager.getLogger(ManagedSynonymTokenFilterFactory.class);

    private final String filterName;
    private final Environment environment;

    public ManagedSynonymTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings)
        throws IOException {
        super(indexSettings, name, settings);
        this.environment = env;
        this.filterName = name;

        // this is return the existing file, or create a new one
        ManagedSynonymStore.getInstance().getOrCreateSynonymFile(name);

        // TODO: update file with stored cache information if needed
    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return AnalysisMode.SEARCH_TIME;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException("Call createPerAnalyzerSynonymFactory to specialize this factory for an analysis chain first");
    }

    @Override
    public TokenFilterFactory getChainAwareTokenFilterFactory(
        TokenizerFactory tokenizer,
        List<CharFilterFactory> charFilters,
        List<TokenFilterFactory> previousTokenFilters,
        Function<String, TokenFilterFactory> allFilters
    ) {
        final Analyzer analyzer = buildSynonymAnalyzer(tokenizer, charFilters, previousTokenFilters, allFilters);
        final SynonymMap synonyms = buildSynonyms(analyzer, getRulesFromSettings(environment));
        final String name = name();
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return synonyms.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonyms, false);
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                // In order to allow chained synonym filters, we return IDENTITY here to
                // ensure that synonyms don't get applied to the synonym map itself,
                // which doesn't support stacked input tokens
                return IDENTITY_FILTER;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return AnalysisMode.SEARCH_TIME;
            }
        };
    }

    Analyzer buildSynonymAnalyzer(
        TokenizerFactory tokenizer,
        List<CharFilterFactory> charFilters,
        List<TokenFilterFactory> tokenFilters,
        Function<String, TokenFilterFactory> allFilters
    ) {
        return new CustomAnalyzer(
            tokenizer,
            charFilters.toArray(new CharFilterFactory[0]),
            tokenFilters.stream().map(TokenFilterFactory::getSynonymFilter).toArray(TokenFilterFactory[]::new)
        );
    }

    SynonymMap buildSynonyms(Analyzer analyzer, Reader rules) {
        try {
            logger.info("Building managed synonyms for " + this.filterName);
            SynonymMap.Builder parser = new ManagedSynonymParser(true, analyzer);
            ((ManagedSynonymParser) parser).parse(rules);
            return parser.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to build synonyms", e);
        }
    }

    Reader getRulesFromSettings(Environment env) {
        logger.info("Getting managed synonyms for " + this.filterName);
        var synonyms = ManagedSynonymStore.getInstance().getSynonymFile(this.filterName);
        if (synonyms == null) {
            throw new IllegalArgumentException("could not find synonym store for: " + this.filterName);
        }

        var rulesList = synonyms.getAll();
        logger.info("Found " + rulesList.size() + " managed synonym set(s)");
        StringBuilder sb = new StringBuilder();
        for (ManagedSynonymSet set : rulesList) {
            sb.append(set.synonymsToString()).append(System.lineSeparator());
        }
        return new StringReader(sb.toString());
    }

}
