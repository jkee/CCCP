package ru.yandex.clickhouse.cccp.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import ru.yandex.clickhouse.cccp.cluster.ClusterNode;
import ru.yandex.clickhouse.cccp.cluster.DatasetConfiguration;
import ru.yandex.clickhouse.cccp.cluster.Region;
import ru.yandex.clickhouse.cccp.index.IndexConfig;
import ru.yandex.clickhouse.cccp.index.IndexRange;
import ru.yandex.clickhouse.cccp.index.IndexType;
import ru.yandex.clickhouse.cccp.index.IndexTypes;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Zookeeper realization of cluster db service, the only one
 * Created by Jkee on 29.10.2016.
 */
public class ZKClusterDBService implements ClusterDBService {

    static final Logger logger = Logger.getLogger(ZKClusterDBService.class);

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
    public void initCluster() {
        try {
            if (zk.exists('/' + clusterName, false) == null) {
                // Create cluster node and other children
                Transaction transaction = zk.transaction();

                transaction.create('/' + clusterName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                transaction.create('/' + clusterName + "/parameters", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                transaction.create('/' + clusterName + "/datasets", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                transaction.create('/' + clusterName + "/nodes", "[]".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                transaction.commit();
                logger.info("Cluster initialized: " + clusterName);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<ClusterNode> getNodes() {
        String nodesPath = '/' + clusterName + "/nodes";
        try {
            byte[] nodesJson = zk.getData(nodesPath, false, null);
            return mapper.readValue(nodesJson, new TypeReference<Set<ClusterNode>>() { });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> getDatasets() {
        try {
            return Sets.newHashSet(zk.getChildren('/' + clusterName + "/datasets", false));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveNodes(Set<ClusterNode> nodes) {
        String nodesPath = '/' + clusterName + "/nodes";
        try {
            byte[] nodesJson = mapper.writeValueAsBytes(nodes);
            zk.setData(nodesPath, nodesJson, -1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DatasetConfiguration loadConfiguration(String datasetName) {

        String paramsPath = '/' + clusterName + "/datasets/" + datasetName + "/parameters";
        String indexPath = '/' + clusterName + "/datasets/" + datasetName + "/index";

        DatasetConfiguration configuration = new DatasetConfiguration();
        configuration.setClusterName(clusterName);
        configuration.setDatasetName(datasetName);

        try {
            byte[] replicationFactorBytes = zk.getData(paramsPath + "/replication_factor", false, null);
            configuration.setReplicationFactor(Integer.valueOf(new String(replicationFactorBytes)));

            byte[] maxTabletSizeBytes = zk.getData(paramsPath + "/max_tablet_size", false, null);
            configuration.setMaxTabletSize(Integer.valueOf(new String(maxTabletSizeBytes)));

            byte[] typesJson = zk.getData(indexPath, false, null);
            List<String> typesString = mapper.readValue(typesJson, new TypeReference<List<String>>() {});
            List<IndexType<?>> types = typesString.stream().map(IndexTypes::fromID).collect(Collectors.toList());
            configuration.setConfig(new IndexConfig(types));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return configuration;
    }

    @Override
    public void setConfiguration(DatasetConfiguration configuration) {
        Preconditions.checkArgument(configuration.getClusterName().equals(clusterName), "Cluster name mismatch");
        try {
            String datasetName = configuration.getDatasetName();

            ZKUtils.createIfNotExists(zk, '/' + clusterName + "/datasets/" + datasetName);

            String paramsPath = '/' + clusterName + "/datasets/" + datasetName + "/parameters";
            String indexPath = '/' + clusterName + "/datasets/" + datasetName + "/index";
            String tablesPath = '/' + clusterName + "/datasets/" + datasetName + "/tables";

            ZKUtils.createIfNotExists(zk, paramsPath);
            ZKUtils.createIfNotExists(zk, indexPath);
            ZKUtils.createIfNotExists(zk, tablesPath);

            ZKUtils.createIfNotExists(zk, paramsPath + "/replication_factor");
            ZKUtils.createIfNotExists(zk, paramsPath + "/max_tablet_size");

            List<String> types = configuration.getConfig().getTypes().stream()
                    .map(IndexType::getID)
                    .collect(Collectors.toList());
            byte[] typesJson = mapper.writeValueAsBytes(types);

            Transaction transaction = zk.transaction();

            transaction.setData(paramsPath + "/replication_factor", String.valueOf(configuration.getReplicationFactor()).getBytes(), -1);
            transaction.setData(paramsPath + "/max_tablet_size", String.valueOf(configuration.getMaxTabletSize()).getBytes(), -1);

            transaction.setData(indexPath, typesJson, -1);

            transaction.commit();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /*
    *
    regions
        *region_<incremental_num>
            left_bound
            right_bound
            nodes
                *node_host
    *
    * */
    public List<Region> loadRegions(String datasetName) {

        return null;
    }

    public void updateRegion(String datasetName, Region region) {
    }


    public Region addRegion(String datasetName, List<ClusterNode> nodes, IndexRange indexRange) {

        String regionPath = '/' + clusterName + "/datasets/" + datasetName + "/regions/region_";

        RegionContent content = new RegionContent();
        content.setLeftBound(indexRange.getUpperBounds());
        content.setRightBound(indexRange.getLowerBounds());
        content.setNodes(nodes);

        try {
            byte[] regionContentJson = mapper.writeValueAsBytes(content);
            // get new sequential number
            String newRegionName = zk.create(regionPath, regionContentJson, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            int id = Integer.parseInt(newRegionName.replace("region_", ""));
            return new Region(id, nodes, indexRange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteRegion(String datasetName, Region region) {
    }


    static class RegionContent {
        long[] leftBound;
        long[] rightBound;
        List<ClusterNode> nodes;

        public long[] getLeftBound() {
            return leftBound;
        }

        public void setLeftBound(long[] leftBound) {
            this.leftBound = leftBound;
        }

        public long[] getRightBound() {
            return rightBound;
        }

        public void setRightBound(long[] rightBound) {
            this.rightBound = rightBound;
        }

        public List<ClusterNode> getNodes() {
            return nodes;
        }

        public void setNodes(List<ClusterNode> nodes) {
            this.nodes = nodes;
        }
    }

}
