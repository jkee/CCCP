package ru.yandex.clickhouse.cccp.api;

import ru.yandex.clickhouse.cccp.cluster.Region;

import java.util.List;

/**
 * Created by Jkee on 29.10.2016.
 */
public interface ClusterService {

    // list of tablets
    List<Region> getRegions();

    // create some tablet for new index range


}
