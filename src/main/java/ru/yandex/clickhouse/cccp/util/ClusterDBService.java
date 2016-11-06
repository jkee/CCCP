package ru.yandex.clickhouse.cccp.util;

import ru.yandex.clickhouse.cccp.cluster.DatasetConfiguration;
import ru.yandex.clickhouse.cccp.cluster.Region;

import java.util.List;

/**
 * Storage for metadata
 * Created by Jkee on 29.10.2016.
 */
public interface ClusterDBService {

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
    void updateRegion(Region region);

    void addRegion(Region region);

    void deleteRegion(Region region);

}
