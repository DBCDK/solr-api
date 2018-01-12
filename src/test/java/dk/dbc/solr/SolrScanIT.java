/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SolrScanIT extends SolrCloud {
    private static final String COLLECTION = "scantest";
    private static CloudSolrClient cloudSolrClient;

    @BeforeClass
    public static void createScanTestCollection() throws IOException, SolrServerException {
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
    public void scan() throws IOException, SolrServerException {
        final List<TermsResponse.Term> expectedTerms = new ArrayList<>(5);
        expectedTerms.add(new TermsResponse.Term("George R.R. Martin", 3));
        expectedTerms.add(new TermsResponse.Term("Glen Cook", 1));
        expectedTerms.add(new TermsResponse.Term("Isaac Asimov", 1));
        expectedTerms.add(new TermsResponse.Term("Lloyd Alexander", 2));
        expectedTerms.add(new TermsResponse.Term("Orson Scott Card", 1));

        final TermsResponse response = new SolrScan(cloudSolrClient, "scantest")
                .withField("author")
                .withUpper("Ow")
                .withUpperInclusive(true)
                .withLimit(30)
                .withSort(SolrScan.SortType.INDEX)
                .execute();

        final List<TermsResponse.Term> terms = response.getTermMap().get("author");
        assertThat("number of terms", terms.size(), is(5));
        int i = 0;
        for (TermsResponse.Term term : terms) {
            assertThat("term " + i, term.getTerm(), is(expectedTerms.get(i).getTerm()));
            assertThat("term " + i + " frequency", term.getFrequency(), is(expectedTerms.get(i).getFrequency()));
            i++;
        }
    }
}
