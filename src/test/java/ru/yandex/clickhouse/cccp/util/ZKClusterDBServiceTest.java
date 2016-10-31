package ru.yandex.clickhouse.cccp.util;

import com.google.common.collect.Sets;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.clickhouse.cccp.cluster.ClusterConfiguration;
import ru.yandex.clickhouse.cccp.cluster.ClusterNode;

/**
 * Some ZK tests
 * Require zookeeper
 * Created by Jkee on 31.10.2016.
 */
public class ZKClusterDBServiceTest {

    static final Logger logger = Logger.getLogger(ZKClusterDBServiceTest.class);

    @Test
    public void saveAndLoad() throws Exception {

        BasicConfigurator.configure();

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

        ZKClusterDBService clusterDBService = new ZKClusterDBService("jkee.org", 2181, "ananas");

        clusterDBService.setConfiguration(configuration);
        ClusterConfiguration loaded = clusterDBService.loadConfiguration();

        Assert.assertEquals(configuration.getClusterName(), loaded.getClusterName());
        Assert.assertEquals(configuration.getMaxTabletSize(), loaded.getMaxTabletSize());
        Assert.assertEquals(configuration.getReplicationFactor(), loaded.getReplicationFactor());
        Assert.assertEquals(configuration.getNodes(), loaded.getNodes());

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

    }

}