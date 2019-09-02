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
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
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

    public SolrSearch withFields(String... fields) {
        solrQuery.setFields(fields);
        return this;
    }

    public String getFields() {
        return solrQuery.getFields();
    }

    public QueryResponse execute() throws IOException, SolrServerException {
        final QueryRequest request = new QueryRequest(solrQuery);
        return request.process(solrClient, collection);
    }

    /**
     * Fetches (a potentially very large number of) sorted results as an
     * iterable result set using the Solr cursor mechanism
     * @return iterable result set
     * @throws SolrServerException On failure to advance the cursor based result set
     */
    public ResultSet executeForCursorBasedIteration() throws SolrServerException {
        return new ResultSet();
    }

    public class ResultSet implements Iterable<SolrDocument> {
        private final long size;
        private Iterator<SolrDocument> documents;
        private String nextCursorMark;
        private String cursorMark = CursorMarkParams.CURSOR_MARK_START;

        ResultSet() throws SolrServerException {
            size = fetchDocuments().getNumFound();
        }

        public long getSize() {
            return size;
        }

        @Override
        public Iterator<SolrDocument> iterator() {
            return new Iterator<SolrDocument>() {
                @Override
                public boolean hasNext() {
                    if (!documents.hasNext()
                            && !cursorMark.equals(nextCursorMark)) {
                        cursorMark = nextCursorMark;
                        try {
                            fetchDocuments();
                        } catch (SolrServerException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                    return documents.hasNext();
                }

                @Override
                public SolrDocument next() {
                    return documents.next();
                }
            };
        }

        private SolrDocumentList fetchDocuments() throws SolrServerException {
            solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
            try {
                final QueryResponse response = execute();
                nextCursorMark = response.getNextCursorMark();
                final SolrDocumentList results = response.getResults();
                this.documents = results.iterator();
                return results;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
