package ru.yandex.clickhouse.cccp.api;

import ru.yandex.clickhouse.cccp.cluster.Region;
import ru.yandex.clickhouse.cccp.index.IndexRange;

import java.util.Map;
import java.util.Set;

/**
 * API to nodes state and operations
 * Created by Jkee on 29.10.2016.
 */
public interface NodesService {

    Set<IndexRange> getRegions();
    Map<IndexRange, Long> getRegionSizes();

}
