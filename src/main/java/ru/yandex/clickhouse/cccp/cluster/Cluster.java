package ru.yandex.clickhouse.cccp.cluster;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ru.yandex.clickhouse.cccp.api.ClusterService;
import ru.yandex.clickhouse.cccp.api.DatasetService;
import ru.yandex.clickhouse.cccp.util.ClusterDBService;

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

    public void init() {
        // load cluster info
        dbService.initCluster();
        nodes = dbService.getNodes();
        // load datasets
        Set<String> datasetNames = dbService.getDatasets();
        for (String datasetName : datasetNames) {
            Dataset dataset = createDataset(datasetName);
            datasets.put(datasetName, dataset);
        }
    }

    private Dataset createDataset(String datasetName) {
        Dataset dataset = new Dataset();
        dataset.setCluster(this);
        dataset.setClusterDBService(dbService);
        dataset.initFromService(datasetName);
        return dataset;
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
    public DatasetService createDataset(DatasetConfiguration configuration) {
        if (datasets.containsKey(configuration.getDatasetName())) {
            throw new IllegalArgumentException("Dataset already exists: " + configuration.getDatasetName());
        }
        dbService.setConfiguration(configuration);

        Dataset dataset = createDataset(configuration.getDatasetName());

        datasets.put(configuration.getDatasetName(), dataset);
        return dataset;
    }

    @Override
    public void addNode(String host, String datacenter) {
        ClusterNode node = new ClusterNode(datacenter, host);
        if (nodes.contains(node)) {
            throw new IllegalArgumentException("Cluster already contains node: " + host);
        }
        nodes.add(node);
        dbService.saveNodes(nodes);
    }

    Set<ClusterNode> getNodes() {
        return nodes;
    }

}
