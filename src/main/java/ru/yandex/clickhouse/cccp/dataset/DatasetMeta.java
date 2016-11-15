package ru.yandex.clickhouse.cccp.dataset;

import ru.yandex.clickhouse.cccp.index.IndexConfig;

import java.util.List;

/**
 * Dataset is number of tables with same distribution index
 * Created by Jkee on 05.11.2016.
 */
public class DatasetMeta {

    private String name;
    private IndexConfig indexConfig;
    private List<TableMeta> tables;

    public DatasetMeta(String name, IndexConfig indexConfig, List<TableMeta> tables) {
        if (name.contains(";")) {
            throw new IllegalArgumentException("Don't try to hijack me boy");
        }
        this.name = name;
        this.indexConfig = indexConfig;
        this.tables = tables;
    }

    public String getName() {
        return name;
    }

    public IndexConfig getIndexConfig() {
        return indexConfig;
    }

    public List<TableMeta> getTables() {
        return tables;
    }
}
