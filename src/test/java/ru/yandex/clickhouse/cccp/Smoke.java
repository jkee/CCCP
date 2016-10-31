package ru.yandex.clickhouse.cccp;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import ru.yandex.clickhouse.cccp.cluster.Cluster;
import ru.yandex.clickhouse.cccp.cluster.ClusterConfiguration;
import ru.yandex.clickhouse.cccp.cluster.ClusterNode;
import ru.yandex.clickhouse.cccp.cluster.Region;
import ru.yandex.clickhouse.cccp.index.IndexRange;
import ru.yandex.clickhouse.cccp.util.ClusterDBService;
import ru.yandex.clickhouse.cccp.util.ZKClusterDBService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Jkee on 29.10.2016.
 */
public class Smoke {

    static final Logger logger = Logger.getLogger(Smoke.class);

    static ClusterDBService service = new ClusterDBService() {

        Map<IndexRange, Region> savedRegions = new HashMap<>();

        public ClusterConfiguration loadConfiguration() {
            ClusterConfiguration configuration = new ClusterConfiguration();
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
            return configuration;
        }

        @Override
        public void setConfiguration(ClusterConfiguration configuration) {

        }

        public List<Region> loadRegions() {
            return new ArrayList<>(savedRegions.values());
        }

        public void updateRegion(Region region) {
            Region savedRegion = savedRegions.get(region.getIndexRange());
            if (savedRegion == null) {
                throw new IllegalArgumentException("No such region: " + region.getIndexRange());
            }
            savedRegions.put(region.getIndexRange(), region);
        }

        public void addRegion(Region region) {
            if (savedRegions.containsKey(region.getIndexRange())) {
                throw new IllegalArgumentException("Region already exists: " + region.getIndexRange());
            }
            savedRegions.put(region.getIndexRange(), region);
        }

        public void deleteRegion(Region region) {
            if (!savedRegions.containsKey(region.getIndexRange())) {
                throw new IllegalArgumentException("No such region: " + region.getIndexRange());
            }
            savedRegions.remove(region.getIndexRange());
        }
    };

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Cluster cluster = new Cluster();
        cluster.setClusterDBService(service);
        cluster.initFromService();

        ZooKeeper zk = new ZooKeeper("jkee.org:2181", 500, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.info("watch from ZK: " + event.toString());
            }
        });

        if (zk.exists("/z1", false) == null) {
            zk.create("/z1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        if (zk.exists("/z1/z2", false) == null) {
            zk.create("/z1/z2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        if (zk.exists("/z1/z2/z3", false) == null) {
            zk.create("/z1/z2/z3", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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

        ZKClusterDBService clusterDBService = new ZKClusterDBService("jkee.org", 2181, "ananas");
        clusterDBService.setConfiguration(configuration);


        ClusterConfiguration clusterConfiguration = clusterDBService.loadConfiguration();
        // todo compare

        System.out.println("Smoked successfully");
    }
}
