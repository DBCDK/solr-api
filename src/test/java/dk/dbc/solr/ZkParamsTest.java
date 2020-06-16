/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ZkParamsTest {
    @Test
    public void testParse() {
        final List<String> expectedHosts = Arrays.asList("host1.example.com:1234", "host2.example.com:5678");
        final String expectedChroot = "/root";

        final ZkParams zkParams = ZkParams.create(String.join(",", expectedHosts) + expectedChroot);
        assertThat("zkHosts", zkParams.getZkHosts(), is(expectedHosts));
        assertThat("zkChroot", zkParams.getZkChroot().get(), is(expectedChroot));
    }
}