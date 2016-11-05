package ru.yandex.clickhouse.cccp.dataset;

import ru.yandex.clickhouse.cccp.index.IndexConfig;

import java.util.List;

/**
 * Dataset is number of tables with same distribution index
 * Created by Jkee on 05.11.2016.
 */
public class Dataset {

    private String name;
    private IndexConfig indexConfig;
    private List<Table> tables;

    public Dataset(String name, IndexConfig indexConfig, List<Table> tables) {
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

    public List<Table> getTables() {
        return tables;
    }
}
