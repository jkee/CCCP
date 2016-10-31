package ru.yandex.clickhouse.cccp.cluster;

import ru.yandex.clickhouse.cccp.api.ClusterService;
import ru.yandex.clickhouse.cccp.util.ClusterDBService;

import java.util.List;

/**
 * Created by Jkee on 29.10.2016.
 */
public class Cluster implements ClusterService {

    private ClusterConfiguration configuration;

    private ClusterDBService clusterDBService;

    // todo index
    private List<Region> regions;

    public void initFromService() {
        configuration = clusterDBService.loadConfiguration();
        regions = clusterDBService.loadRegions();
    }

    public ClusterConfiguration getConfiguration() {
        return configuration;
    }

    public void setClusterDBService(ClusterDBService clusterDBService) {
        this.clusterDBService = clusterDBService;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public void setRegions(List<Region> regions) {
        this.regions = regions;
    }
}
