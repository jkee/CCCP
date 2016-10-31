package ru.yandex.clickhouse.cccp.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooKeeper;
import ru.yandex.clickhouse.cccp.cluster.ClusterConfiguration;
import ru.yandex.clickhouse.cccp.cluster.ClusterNode;
import ru.yandex.clickhouse.cccp.cluster.Region;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Zookeeper realization of cluster db service, the only one
 * Created by Jkee on 29.10.2016.
 */
public class ZKClusterDBService implements ClusterDBService {

    private ZooKeeper zk;

    private ObjectMapper mapper = new ObjectMapper();

    private String clusterName;

    public ZKClusterDBService(String host, int port, String clusterName) {
        this.clusterName = clusterName;
        try {
            // no listener for now
            zk = new ZooKeeper(host + ':' + port, 500, event -> {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClusterConfiguration loadConfiguration() {

        String paramsPath = '/' + clusterName + "/configuration/parameters";
        String nodesPath = '/' + clusterName + "/configuration/nodes";

        ClusterConfiguration configuration = new ClusterConfiguration();
        configuration.setClusterName(clusterName);

        try {
            byte[] replicationFactorBytes = zk.getData(paramsPath + "/replicationFactor", false, null);
            configuration.setReplicationFactor(Integer.valueOf(new String(replicationFactorBytes)));

            byte[] maxTabletSizeBytes = zk.getData(paramsPath + "/maxTabletSize", false, null);
            configuration.setMaxTabletSize(Integer.valueOf(new String(maxTabletSizeBytes)));

            byte[] nodesJson = zk.getData(nodesPath, false, null);
            Set<ClusterNode> nodes = mapper.readValue(nodesJson, new TypeReference<Set<ClusterNode>>() { });
            configuration.setNodes(nodes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return configuration;
    }

    @Override
    public void setConfiguration(ClusterConfiguration configuration) {
        Preconditions.checkArgument(configuration.getClusterName().equals(clusterName), "Cluster name mismatch");
        try {
            ZKUtil.createIfNotExists(zk, '/' + clusterName);
            ZKUtil.createIfNotExists(zk, '/' + clusterName + "/configuration");

            String paramsPath = '/' + clusterName + "/configuration/parameters";
            String nodesPath = '/' + clusterName + "/configuration/nodes";

            ZKUtil.createIfNotExists(zk, paramsPath);
            ZKUtil.createIfNotExists(zk, nodesPath);

            ZKUtil.createIfNotExists(zk, paramsPath + "/replicationFactor");
            ZKUtil.createIfNotExists(zk, paramsPath + "/maxTabletSize");

            byte[] nodesJson = mapper.writeValueAsBytes(configuration.getNodes());

            Transaction transaction = zk.transaction();

            transaction.setData(paramsPath + "/replicationFactor", String.valueOf(configuration.getReplicationFactor()).getBytes(), -1);
            transaction.setData(paramsPath + "/maxTabletSize", String.valueOf(configuration.getMaxTabletSize()).getBytes(), -1);

            transaction.setData(nodesPath, nodesJson, -1);

            transaction.commit();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Region> loadRegions() {
        return null;
    }

    public void updateRegion(Region region) {
    }

    public void addRegion(Region region) {
    }

    public void deleteRegion(Region region) {
    }

}
