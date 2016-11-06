package ru.yandex.clickhouse.cccp.api;

import ru.yandex.clickhouse.cccp.cluster.DatasetConfiguration;

/**
 * Cluster level service
 * Created by Jkee on 29.10.2016.
 */
public interface ClusterService {

    String getName();


    // DATASET API

    DatasetService getDataset(String datasetName);

    DatasetService createDataset(String datasetName, DatasetConfiguration configuration);

    // NODES API

    /**
     * Add new node to cluster
     * Adding already existing host will raise an error
     * @param host host name (FQDN)
     * @param datacenter datacenter ID for cross-datacenter environment
     */
    void addNode(String host, String datacenter);

    // is it really required to remove nodes? Actually, yes.


}
