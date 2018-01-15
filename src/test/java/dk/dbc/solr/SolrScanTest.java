/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;


public class SolrScanTest {
    private static final SolrClient SOLR_CLIENT = mock(SolrClient.class);
    private static final String COLLECTION = "scanTest";

    @Test
    public void withFieldClearsExistingValues() {
        final SolrScan solrScan = new SolrScan(SOLR_CLIENT, COLLECTION)
                .withField("marc.245a")
                .withField("marc.245b");

        assertThat(solrScan.getField(), is("marc.245b"));
    }

    @Test
    public void withRegexFlagClearsExistingValues() {
        final SolrScan solrScan = new SolrScan(SOLR_CLIENT, COLLECTION)
                .withRegexFlag(SolrScan.RegexFlag.CASE_INSENSITIVE)
                .withRegexFlag(SolrScan.RegexFlag.COMMENTS, SolrScan.RegexFlag.MULTILINE);

        assertThat(solrScan.getRegexFlag(),
                contains(SolrScan.RegexFlag.COMMENTS, SolrScan.RegexFlag.MULTILINE));
    }
}