/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


public class SolrCloud {
    @ClassRule
    public static TemporaryFolder solrHome = new TemporaryFolder();

    static MiniSolrCloudCluster miniSolrCloudCluster;

    @BeforeClass
    public static void startCluster() throws Exception {
        // Without this property solr8 tests result in IOException: 6/invalid_frame_length
        System.setProperty("jetty.testMode", "true");
        miniSolrCloudCluster = new MiniSolrCloudCluster(1, null,
                FileSystems.getDefault().getPath(solrHome.getRoot().getAbsolutePath()),
                MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML, null, null);

        pingCluster();
    }

    @AfterClass
    public static void stopCluster() throws Exception {
        if (miniSolrCloudCluster != null) {
            miniSolrCloudCluster.shutdown();
        }
    }

    public static String getZkAddress() {
        if (miniSolrCloudCluster != null) {
            return miniSolrCloudCluster.getZkServer().getZkAddress();
        }
        return null;
    }

    public static void createCollection(CloudSolrClient cloudSolrClient, String collection,
                                        int numShards, int replicationFactor, File confDir) {
        try {
            if (confDir != null) {
                assertThat("Specified Solr config directory '" + confDir.getAbsolutePath() + "' not found!",
                        confDir.isDirectory(), is(true));

                // upload the configs
                final SolrZkClient zkClient = cloudSolrClient.getZkStateReader().getZkClient();
                final ZkConfigManager zkConfigManager = new ZkConfigManager(zkClient);
                zkConfigManager.uploadConfigDir(confDir.toPath(), collection);
            }

            final int liveNodes = cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes().size();
            final int maxShardsPerNode = (int) Math.ceil(((double) numShards * replicationFactor) / liveNodes);

            final ModifiableSolrParams params = new ModifiableSolrParams();
            params.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name());
            params.set("name", collection);
            params.set("numShards", numShards);
            params.set("replicationFactor", replicationFactor);
            params.set("maxShardsPerNode", maxShardsPerNode);
            params.set("collection.configName", collection);
            final QueryRequest request = new QueryRequest(params);
            request.setPath("/admin/collections");
            cloudSolrClient.request(request);

            verifyReplicas(cloudSolrClient, collection, numShards, replicationFactor);
        } catch (InterruptedException | IOException | KeeperException | SolrServerException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void verifyReplicas(CloudSolrClient cloudSolrClient, String collection,
                                       int numShards, int replicationFactor)
            throws KeeperException, InterruptedException {
        final ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();

        long waitMs = 0L;
        long maxWaitMs = 20 * 1000L;
        boolean allReplicasUp = false;
        waitLoop: while (waitMs < maxWaitMs && !allReplicasUp) {
            Thread.sleep(500L);
            waitMs += 500L;

            zkStateReader.forceUpdateCollection(collection);
            final ClusterState clusterState = zkStateReader.getClusterState();
            final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
            if (docCollection != null) {
                final Collection<Slice> activeSlices = docCollection.getActiveSlices();
                if (activeSlices.size() < numShards) {
                    continue waitLoop;
                }
                for (Slice slice : activeSlices) {
                    final Collection<Replica> replicas = slice.getReplicas();
                    if (replicas.size() != replicationFactor) {
                        continue waitLoop;
                    }
                    for (Replica replica : replicas) {
                        if (replica.getState() != Replica.State.ACTIVE) {
                            continue waitLoop;
                        }
                    }
                }
                allReplicasUp = true;
            }
        }

        if (!allReplicasUp) {
            fail("Didn't see all replicas for " + collection + " come up within " + maxWaitMs + " ms");
        }
    }

    private static void pingCluster() throws IOException {
        try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
                .withZkHost(getZkAddress())
                .build()) {
            cloudSolrClient.connect();

            assertThat("live nodes", cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes().isEmpty(),
                    is(not(true)));

            System.out.println("cluster state: " + cloudSolrClient.getZkStateReader().getClusterState());
        }
    }
}
