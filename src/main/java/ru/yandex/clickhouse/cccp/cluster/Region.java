package ru.yandex.clickhouse.cccp.cluster;

import com.google.common.base.Preconditions;
import ru.yandex.clickhouse.cccp.index.IndexRange;

import java.util.List;

/**
 * Region is a piece of data by index
 * Placed on <code>replication_factor</code> cluster nodes
 * Created by Jkee on 29.10.2016.
 */
public class Region {

    // nodes data is placed on
    private List<ClusterNode> activeNodes;

    // index range
    // index range can't be changed
    private IndexRange indexRange;

    public Region(List<ClusterNode> activeNodes, IndexRange indexRange) {
        Preconditions.checkNotNull(activeNodes);
        Preconditions.checkNotNull(indexRange);
        this.activeNodes = activeNodes;
        this.indexRange = indexRange;
    }

    public List<ClusterNode> getActiveNodes() {
        return activeNodes;
    }

    public IndexRange getIndexRange() {
        return indexRange;
    }
}
