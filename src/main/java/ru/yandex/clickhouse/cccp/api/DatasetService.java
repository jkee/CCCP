package ru.yandex.clickhouse.cccp.api;

import ru.yandex.clickhouse.cccp.cluster.Region;
import ru.yandex.clickhouse.cccp.index.IndexConfig;

import java.util.List;

/**
 * Created by Jkee on 29.10.2016.
 */
public interface DatasetService {

    String getName();

    // list of regions
    List<Region> getRegions();

    // get region for index
    Region getRegion(List<Object> index);

    // get index config
    IndexConfig getIndexConfig();

    // hosts for regionID
    // todo should not be here, take it from ZK and subscribe as writer
    List<String> getHosts(int regionID);


}
