package ru.yandex.clickhouse.cccp.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.clickhouse.cccp.api.ClusterService;
import ru.yandex.clickhouse.cccp.cluster.Cluster;
import ru.yandex.clickhouse.cccp.cluster.ClusterConfiguration;
import ru.yandex.clickhouse.cccp.cluster.ClusterNode;
import ru.yandex.clickhouse.cccp.index.IndexConfig;
import ru.yandex.clickhouse.cccp.index.IndexTypes;
import ru.yandex.clickhouse.cccp.util.ZKClusterDBService;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Created by jkee on 06/11/16.
 */
public class ClusterTest {

    ClusterClient clusterClient;
    ClusterService service;

    ZooKeeper zk;

    @Before
    public void setUp() throws Exception {

        BasicConfigurator.configure();

        zk = new ZooKeeper("jkee.org:2181",500, null);
        if (zk.exists("/testcluster", false) != null) {
            ZKUtil.deleteRecursive(zk, "/testcluster");
        }

        ClusterConfiguration configuration = new ClusterConfiguration();
        configuration.setClusterName("testcluster");
        configuration.setReplicationFactor(1);
        configuration.setMaxTabletSize(100 * 1024); // 100 gb
        configuration.setNodes(Sets.newHashSet(
                new ClusterNode("jkee.org", "jkee01")
        ));
        IndexConfig config = new IndexConfig(Lists.newArrayList(
                IndexTypes.MONTH,
                IndexTypes.UInt32,
                IndexTypes.UInt64
        ));
        configuration.setConfig(config);

        ZKClusterDBService clusterDBService = new ZKClusterDBService("jkee.org", 2181, "testcluster");

        clusterDBService.setConfiguration(configuration);

        Cluster cluster = new Cluster();
        cluster.setClusterDBService(clusterDBService);

        cluster.initFromService();

        clusterClient = new ClusterClient("jkee.org", 2181, cluster);
        clusterClient.dataset = "testcluster";
        clusterClient.table = "hits";

    }

    @Test
    public void testAddData() throws Exception {

        List<TestHit> hits = Lists.newArrayList(
                new TestHit(LocalDateTime.of(2016, 1, 1, 1, 1), 101024, 4242, "ololo1"),
                new TestHit(LocalDateTime.of(2016, 1, 1, 1, 2), 101024, 4242, "ololo2"),
                new TestHit(LocalDateTime.of(2016, 1, 1, 1, 3), 101024, 4242, "ololo3"),
                new TestHit(LocalDateTime.of(2016, 1, 1, 1, 4), 101024, 4242, "ololo4"),
                new TestHit(LocalDateTime.of(2016, 1, 1, 4, 1), 101024, 4243, "ololo5"),
                new TestHit(LocalDateTime.of(2016, 1, 1, 3, 1), 101024, 4243, "ololo6"),
                new TestHit(LocalDateTime.of(2016, 1, 1, 2, 1), 101024, 4243, "ololo7"),
                new TestHit(LocalDateTime.of(2016, 1, 1, 1, 1), 101024, 4243, "ololo8")
        );

        clusterClient.insertData(hits);

    }
}
