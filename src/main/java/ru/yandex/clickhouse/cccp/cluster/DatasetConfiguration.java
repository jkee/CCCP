package ru.yandex.clickhouse.cccp.cluster;

import ru.yandex.clickhouse.cccp.index.IndexConfig;

import java.util.Set;

/**
 * Hardware configuration and parameters
 * Created by Jkee on 29.10.2016.
 */
public class DatasetConfiguration {

    // unique cluster name string
    private String clusterName;

    private String datasetName;

    // only Metrica cluster configuration for now
    // simplified for one layer

    // all servers
    private Set<ClusterNode> nodes;

    // index
    private IndexConfig config;

    // configuration parameters
    private int replicationFactor;

    // max desired tablet size in mbytes
    private long maxTabletSize;


    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public Set<ClusterNode> getNodes() {
        return nodes;
    }

    public void setNodes(Set<ClusterNode> nodes) {
        this.nodes = nodes;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public long getMaxTabletSize() {
        return maxTabletSize;
    }

    public void setMaxTabletSize(long maxTabletSize) {
        this.maxTabletSize = maxTabletSize;
    }

    public IndexConfig getConfig() {
        return config;
    }

    public void setConfig(IndexConfig config) {
        this.config = config;
    }
}
