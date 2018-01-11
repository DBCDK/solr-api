/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Index scans using the Solr Terms Component
 * @see <a href="https://lucene.apache.org/solr/guide/6_6/the-terms-component.html">Terms Component</a>
 */
public class SolrScan {
    private static final String HANDLER = "/terms";

    private final SolrClient solrClient;
    private final SolrQuery solrQuery;
    private final String collection;

    /**
     * @see <a href="https://docs.oracle.com/javase/tutorial/essential/regex/pattern.html">Regex flags</a>
     */
    public enum RegexFlag {
        CANON_EQ,
        CASE_INSENSITIVE,
        COMMENTS,
        DOTALL,
        LITERAL,
        MULTILINE,
        UNICODE_CASE,
        UNIX_LINES
    }

    public enum SortType {
        COUNT,
        INDEX
    }

    public SolrScan(CloudSolrClient solrClient, String collection) {
        this((SolrClient) solrClient, collection);
        solrClient.connect();
        final List<String> shardUrls = getShardUrls();
        if (shardUrls.size() > 1) {
            // The terms component does not seem to work
            // transparently with CloudSolrClient so we still
            // need to set it up for distributed operation
            solrQuery.add("shards.qt", HANDLER);
            solrQuery.add("shards", String.join(",", shardUrls));
        }
    }

    public SolrScan(SolrClient solrClient, String collection) {
        this.solrClient = solrClient;
        this.solrQuery = new SolrQuery();
        this.collection = collection;
        solrQuery.setRequestHandler(HANDLER);
    }

    public SolrScan withField(String field) {
        solrQuery.remove("terms.fl");
        solrQuery.addTermsField(field);
        return this;
    }

    public String getField() {
        final String[] fields = solrQuery.getTermsFields();
        if (fields.length == 1) {
            return fields[0];
        } else if (fields.length > 1) {
            throw new IllegalStateException("Multiple fields contained in scan");
        }
        return null;
    }

    public SolrScan withLimit(int limit) {
        solrQuery.setTermsLimit(limit);
        return this;
    }

    public int getLimit() {
        return solrQuery.getTermsLimit();
    }

    public SolrScan withLower(String lower) {
        solrQuery.setTermsLower(lower);
        return this;
    }

    public String getLower() {
        return solrQuery.getTermsLower();
    }

    public SolrScan withLowerInclusive(boolean inclusive) {
        solrQuery.setTermsLowerInclusive(inclusive);
        return this;
    }

    public boolean isLowerInclusive() {
        return solrQuery.getTermsLowerInclusive();
    }

    public SolrScan withMinCount(int minCount) {
        solrQuery.setTermsMinCount(minCount);
        return this;
    }

    public int getMinCount() {
        return solrQuery.getTermsMinCount();
    }

    public SolrScan withMaxCount(int maxCount) {
        solrQuery.setTermsMaxCount(maxCount);
        return this;
    }

    public int getMaxCount() {
        return solrQuery.getTermsMaxCount();
    }

    public SolrScan withPrefix(String prefix) {
        solrQuery.setTermsPrefix(prefix);
        return this;
    }

    public String getPrefix() {
        return solrQuery.getTermsPrefix();
    }

    public SolrScan withRaw(boolean raw) {
        solrQuery.setTermsRaw(raw);
        return this;
    }

    public boolean isRaw() {
        return solrQuery.getTermsRaw();
    }

    public SolrScan withRegex(String regex) {
        solrQuery.setTermsRegex(regex);
        return this;
    }

    public String getRegex() {
        return solrQuery.getTermsRegex();
    }

    public SolrScan withRegexFlag(RegexFlag... flags) {
        solrQuery.remove("terms.regex.flag");
        Arrays.stream(flags).forEach(
                flag -> solrQuery.setTermsRegexFlag(flag.name().toLowerCase()));
        return this;
    }

    public List<RegexFlag> getRegexFlag() {
        return Arrays.stream(solrQuery.getTermsRegexFlags())
                .map(param -> RegexFlag.valueOf(param.toUpperCase()))
                .collect(Collectors.toList());
    }

    public SolrScan withSort(SortType sortType) {
        solrQuery.setTermsSortString(sortType.name().toLowerCase());
        return this;
    }

    public SortType getSort() {
        return SortType.valueOf(solrQuery.getTermsSortString().toUpperCase());
    }

    public SolrScan withUpper(String upper) {
        solrQuery.setTermsUpper(upper);
        return this;
    }

    public String getUpper() {
        return solrQuery.getTermsUpper();
    }

    public SolrScan withUpperInclusive(boolean inclusive) {
        solrQuery.setTermsUpperInclusive(inclusive);
        return this;
    }

    public boolean isUpperInclusive() {
        return solrQuery.getTermsUpperInclusive();
    }

    public TermsResponse execute() throws IOException, SolrServerException {
        final QueryRequest request = new QueryRequest(solrQuery);
        final QueryResponse response = request.process(solrClient, collection);
        if (response != null) {
            return response.getTermsResponse();
        }
        return null;
    }

    private List<String> getShardUrls() {
        final List<String> urls = new ArrayList<>();
        final ZkStateReader zkStateReader = ((CloudSolrClient) solrClient).getZkStateReader();
        final DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
        if (docCollection != null) {
            for (Slice slice : docCollection.getSlices()) {
                final List<String> replicas = new ArrayList<>();
                for (Replica replica : slice.getReplicas()) {
                    if (replica.getState() == Replica.State.ACTIVE) {
                        replicas.add(replica.getCoreUrl());
                    }
                }
                if (!replicas.isEmpty()) {
                    // pipe '|' indicates that solr should choose a
                    // shard replica randomly
                    urls.add(String.join("|", replicas));
                }
            }
        }
        return urls;
    }
}