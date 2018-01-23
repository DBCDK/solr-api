/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SolrSearchIT extends SolrCloud {
    private static final String COLLECTION = "searchtest";
    private static CloudSolrClient cloudSolrClient;

    @BeforeClass
    public static void createSearchTestCollection() throws IOException, SolrServerException {
        final File confDir = new File("src/test/resources/conf");
        cloudSolrClient = new CloudSolrClient.Builder().withZkHost(getZkAddress()).build();
        cloudSolrClient.connect();
        createCollection(cloudSolrClient, COLLECTION, 2, 1, confDir);
        try (final InputStream inputStream = new FileInputStream("src/test/resources/books.json")) {
            final JsonUpdateRequest request = new JsonUpdateRequest(inputStream);
            request.process(cloudSolrClient, COLLECTION);
        }
        cloudSolrClient.commit(COLLECTION);
    }

    @AfterClass
    public static void closeClient() throws IOException {
        if (cloudSolrClient != null) {
            cloudSolrClient.close();
        }
    }

    @Test
    public void search() throws IOException, SolrServerException {
        final QueryResponse response = new SolrSearch(cloudSolrClient, COLLECTION)
                .withQuery("title:game")
                .withRows(1)
                .execute();

        final SolrDocumentList results = response.getResults();
        assertThat("number of hits", results.getNumFound(), is(2L));
        assertThat("number of returned rows", results.size(), is(1));
    }
}
