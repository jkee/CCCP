package ru.yandex.clickhouse.cccp.integration;

import com.google.common.collect.Lists;
import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.clickhouse.cccp.api.ClusterService;
import ru.yandex.clickhouse.cccp.api.DatasetService;
import ru.yandex.clickhouse.cccp.cluster.Cluster;
import ru.yandex.clickhouse.cccp.cluster.ClusterNode;
import ru.yandex.clickhouse.cccp.cluster.DatasetConfiguration;
import ru.yandex.clickhouse.cccp.index.IndexConfig;
import ru.yandex.clickhouse.cccp.index.IndexTypes;
import ru.yandex.clickhouse.cccp.util.ClusterDBService;
import ru.yandex.clickhouse.cccp.util.ZKClusterDBService;

import java.util.Set;

/**
 * Created by jkee on 06/11/16.
 */
public class ClusterServiceTest {


    String clusterName = "testcluster";
    ClusterDBService dbService;

    ZooKeeper zk;

    @Before
    public void setUp() throws Exception {

        BasicConfigurator.configure();

        zk = new ZooKeeper("jkee.org:2181",500, null);
        if (zk.exists("/" + clusterName, false) != null) {
            ZKUtil.deleteRecursive(zk, "/" + clusterName);
        }
        dbService = new ZKClusterDBService("jkee.org", 2181, clusterName);
    }

    @After
    public void tearDown() throws Exception {
        ZKUtil.deleteRecursive(zk, "/" + clusterName);
    }

    @Test
    public void testNewCluster() throws Exception {
        // Creating new cluster

        /* Pattern:
        Creating cluster from a scratch

        How that should be done:
        1. Creating empty cluster with name only
        2. Adding all nodes

        */

        ClusterService service = new Cluster(clusterName, dbService);

        // initializing
        service.init();

        // adding nodes
        service.addNode("testnode1.jkee.org", "dc1");
        service.addNode("testnode2.jkee.org", "dc1");

        // checking
        Set<ClusterNode> nodes = dbService.getNodes();
        Assert.assertTrue(nodes.size() == 2);
        Assert.assertTrue(nodes.contains(new ClusterNode("dc1", "testnode1.jkee.org")));
        Assert.assertTrue(nodes.contains(new ClusterNode("dc1", "testnode2.jkee.org")));

        Assert.assertEquals(service.getName(), clusterName);
    }

    @Test
    public void testNewDataset() throws Exception {
        ClusterService service = new Cluster(clusterName, dbService);

        // initializing
        service.init();

        // adding nodes
        service.addNode("testnode1.jkee.org", "dc1");
        service.addNode("testnode2.jkee.org", "dc1");

        // creating dataset
        DatasetConfiguration configuration = new DatasetConfiguration();
        configuration.setClusterName(clusterName);
        configuration.setDatasetName("testdataset");
        configuration.setReplicationFactor(1);
        configuration.setMaxTabletSize(8);
        IndexConfig config = new IndexConfig(Lists.newArrayList(IndexTypes.MONTH));
        configuration.setConfig(config);
        service.createDataset(configuration);

        // checking
        DatasetService testdataset = service.getDataset("testdataset");
        Assert.assertEquals(IndexTypes.MONTH.getID(), testdataset.getIndexConfig().getTypes().get(0).getID());
        Assert.assertEquals("testdataset", testdataset.getName());
    }
}