package ru.yandex.clickhouse.cccp.index;

import java.util.List;

/**
 * Cluster index configuration
 * Created by Jkee on 01.11.2016.
 */
public class IndexConfig {
    private List<IndexType<?>> types;

    public List<IndexType<?>> getTypes() {
        return types;
    }

    public void setTypes(List<IndexType<?>> types) {
        this.types = types;
    }
}
