package ru.yandex.clickhouse.cccp.cluster;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ru.yandex.clickhouse.cccp.api.ClusterService;
import ru.yandex.clickhouse.cccp.api.DatasetService;
import ru.yandex.clickhouse.cccp.util.ClusterDBService;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Map;
import java.util.Set;

/**
 * Cluster entry point
 * Created by Jkee on 29.10.2016.
 */
public class Cluster implements ClusterService {

    // unique cluster name string
    private String clusterName;

    // all servers
    private Set<ClusterNode> nodes = Sets.newHashSet();

    private ClusterDBService dbService;

    private Map<String, Dataset> datasets = Maps.newHashMap();

    public Cluster(String clusterName, ClusterDBService dbService) {
        this.clusterName = clusterName;
        this.dbService = dbService;
    }

    public void initFromDB(String clusterName) {
        // load cluster info, basically it's host list

        // todo should also load datasets
    }

    @Override
    public String getName() {
        return clusterName;
    }

    @Override
    public DatasetService getDataset(String datasetName) {
        if (!datasets.containsKey(datasetName)) {
            throw new IllegalArgumentException("No dataset for name: " + datasetName);
        }
        return datasets.get(datasetName);
    }

    @Override
    public DatasetService createDataset(String datasetName, DatasetConfiguration configuration) {
        // todo
        throw new NotImplementedException();
    }

    @Override
    public void addNode(String host, String datacenter) {
        ClusterNode node = new ClusterNode(datacenter, host);
        throw new NotImplementedException();
    }
}
