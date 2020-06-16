/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SolrFieldAnalysisIT extends SolrCloud {
    private static final String COLLECTION = "fieldanalysistest";
    private static CloudSolrClient cloudSolrClient;

    @BeforeClass
    public static void createFieldAnalysisTestCollection() throws IOException, SolrServerException {
        final File confDir = new File("src/test/resources/conf");
        final ZkParams zkParams = ZkParams.create(getZkAddress());
        cloudSolrClient = new CloudSolrClient.Builder(zkParams.getZkHosts(), zkParams.getZkChroot()).build();
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
    public void byFieldType() throws SolrServerException {
        final SolrFieldAnalysis solrFieldAnalysis = new SolrFieldAnalysis(cloudSolrClient, COLLECTION);
        assertThat(solrFieldAnalysis.byFieldType("text_general", "MyTeRm"),
                is("myterm"));
    }
}
