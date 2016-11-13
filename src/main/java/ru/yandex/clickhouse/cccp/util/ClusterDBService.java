package ru.yandex.clickhouse.cccp.util;

import ru.yandex.clickhouse.cccp.cluster.ClusterNode;
import ru.yandex.clickhouse.cccp.cluster.DatasetConfiguration;
import ru.yandex.clickhouse.cccp.cluster.Region;
import ru.yandex.clickhouse.cccp.index.IndexRange;

import java.util.List;
import java.util.Set;

/**
 * Storage for metadata
 * Created by Jkee on 29.10.2016.
 */
public interface ClusterDBService {

    // Cluster level

    void initCluster();

    void saveNodes(Set<ClusterNode> nodes);

    Set<ClusterNode> getNodes();

    Set<String> getDatasets();

    // Dataset level

    /**
     * Set configuration
     */
    void setConfiguration(DatasetConfiguration configuration);

    /**
     * Loads cluster configuration
     */
    DatasetConfiguration loadConfiguration(String datasetName);

    /**
     * Loads all current regions configuration from database
     */
    List<Region> loadRegions(String datasetName);

    /**
     * Updates current region with new nodes info
     * Region index range can't be changed!
     */
    void updateRegion(String datasetName, Region region);

    Region addRegion(String datasetName, List<ClusterNode> nodes, IndexRange indexRange);

    void deleteRegion(String datasetName, Region region);

}
