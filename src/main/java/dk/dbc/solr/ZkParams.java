/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ZkParams {
    public static ZkParams create(String zkQuorumEndpoint) {
        return new ZkParams(zkQuorumEndpoint);
    }

    /**
     * a List of at least one org.apache.zookeeper.ZooKeeper host and port
     */
    private List<String> zkHosts;

    /**
     *  the path to the root ZooKeeper node containing Solr data.
     *  May be empty if Solr-data is located at the ZooKeeper root.
     */
    private String zkChroot;

    private ZkParams(String zkQuorumEndpoint) {
        if (zkQuorumEndpoint == null || zkQuorumEndpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("Illegal zkQuorumEndpoint'" + zkQuorumEndpoint + "'");
        }
        parseZkQuorumEndpoint(zkQuorumEndpoint);
    }

    // socl-p101.dbc.dk,socl-p102.dbc.dk,socl-p103.dbc.dk,socl-p104.dbc.dk,socl-p201.dbc.dk,socl-p202.dbc.dk,socl-p203.dbc.dk,socl-p204.dbc.dk,socl-p301.dbc.dk,socl-p302.dbc.dk,socl-p303.dbc.dk,socl-p304.dbc.dk,socl-p305.dbc.dk/cisterneRR7

    public List<String> getZkHosts() {
        return zkHosts;
    }

    public Optional<String> getZkChroot() {
        return Optional.ofNullable(zkChroot);
    }

    private void parseZkQuorumEndpoint(String zkQuorumEndpoint) {
        final String[] parts = zkQuorumEndpoint.split("/", 2);
        if (parts.length == 2) {
            zkChroot = "/" + parts[1];
        }
        zkHosts = Arrays.asList(parts[0].split(","));
    }
}
