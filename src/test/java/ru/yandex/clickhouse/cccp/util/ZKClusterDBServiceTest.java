package ru.yandex.clickhouse.cccp.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.clickhouse.cccp.cluster.ClusterConfiguration;
import ru.yandex.clickhouse.cccp.cluster.ClusterNode;
import ru.yandex.clickhouse.cccp.index.IndexConfig;
import ru.yandex.clickhouse.cccp.index.IndexTypes;

/**
 * Some ZK tests
 * Require zookeeper
 * Created by Jkee on 31.10.2016.
 */
public class ZKClusterDBServiceTest {

    static final Logger logger = Logger.getLogger(ZKClusterDBServiceTest.class);

    private ZooKeeper zk;

    @Test
    public void saveAndLoad() throws Exception {

        BasicConfigurator.configure();

        zk = new ZooKeeper("jkee.org:2181",500, null);
        if (zk.exists("/ananas", false) != null) {
            ZKUtil.deleteRecursive(zk, "/ananas");
        }

        ClusterConfiguration configuration = new ClusterConfiguration();
        configuration.setClusterName("ananas");
        configuration.setReplicationFactor(3);
        configuration.setMaxTabletSize(100 * 1024); // 100 gb
        configuration.setNodes(Sets.newHashSet(
                new ClusterNode("redhead", "ananas01"),
                new ClusterNode("redhead", "ananas02"),
                new ClusterNode("redhead", "ananas03"),
                new ClusterNode("ginger", "ananas04"),
                new ClusterNode("ginger", "ananas05"),
                new ClusterNode("ginger", "ananas06"),
                new ClusterNode("bald", "ananas07"),
                new ClusterNode("bald", "ananas08"),
                new ClusterNode("bald", "ananas09")
        ));
        IndexConfig config = new IndexConfig(Lists.newArrayList(
                IndexTypes.MONTH,
                IndexTypes.UInt32,
                IndexTypes.UInt64
        ));
        configuration.setConfig(config);

        ZKClusterDBService clusterDBService = new ZKClusterDBService("jkee.org", 2181, "ananas");

        clusterDBService.setConfiguration(configuration);
        ClusterConfiguration loaded = clusterDBService.loadConfiguration();

        Assert.assertEquals(configuration.getClusterName(), loaded.getClusterName());
        Assert.assertEquals(configuration.getMaxTabletSize(), loaded.getMaxTabletSize());
        Assert.assertEquals(configuration.getReplicationFactor(), loaded.getReplicationFactor());
        Assert.assertEquals(configuration.getNodes(), loaded.getNodes());

        Assert.assertEquals(config.getTypes(), configuration.getConfig().getTypes());

        // reload

        configuration.setReplicationFactor(4);
        configuration.setNodes(Sets.newHashSet(
                new ClusterNode("redhead", "ananas01")
        ));

        clusterDBService.setConfiguration(configuration);
        loaded = clusterDBService.loadConfiguration();

        Assert.assertEquals(configuration.getClusterName(), loaded.getClusterName());
        Assert.assertEquals(configuration.getMaxTabletSize(), loaded.getMaxTabletSize());
        Assert.assertEquals(configuration.getReplicationFactor(), loaded.getReplicationFactor());
        Assert.assertEquals(configuration.getNodes(), loaded.getNodes());

        Assert.assertEquals(config.getTypes(), configuration.getConfig().getTypes());


        org.apache.zookeeper.ZKUtil.deleteRecursive(zk, "/ananas");

    }

}