/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Searches using the Solr SearchHandler
 * @see <a href="https://lucene.apache.org/solr/guide/6_6/common-query-parameters.html#common-query-parameters">Common parameters</a>
 */
public class SolrSearch {
    private final SolrClient solrClient;
    private final SolrQuery solrQuery;
    private final String collection;

    public SolrSearch(SolrClient solrClient, String collection) {
        this.solrClient = solrClient;
        this.solrQuery = new SolrQuery();
        this.collection = collection;
    }

    public SolrSearch withQuery(String q) {
        solrQuery.setQuery(q);
        return this;
    }

    public String getQuery() {
        return solrQuery.getQuery();
    }

    public SolrSearch withRows(int max) {
        solrQuery.setRows(max);
        return this;
    }

    public int getRows() {
        return solrQuery.getRows();
    }

    public SolrSearch withStart(int offset) {
        solrQuery.setStart(offset);
        return this;
    }

    public int getStart() {
        return solrQuery.getStart();
    }

    public SolrSearch withSortClauses(SolrQuery.SortClause... sortClauses) {
        solrQuery.setSorts(Arrays.asList(sortClauses));
        return this;
    }

    public List<SolrQuery.SortClause> getSortClauses() {
        return solrQuery.getSorts();
    }

    public QueryResponse execute() throws IOException, SolrServerException {
        final QueryRequest request = new QueryRequest(solrQuery);
        return request.process(solrClient, collection);
    }
}
