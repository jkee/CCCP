package ru.yandex.clickhouse.cccp.dataset;

/**
 * Table in dataset
 * Table described as:
 *  - Name
 *  - Version
 *  - 'Create' statement
 *
 *  .. todo more here
 *
 * Created by Jkee on 05.11.2016.
 */
public class TableMeta {

    public static final String TABLE_PLACEHOLDER = "%table_name%";

    private String name;
    private int version;

    /**
     * Create statement where table name is <code>TABLE_PLACEHOLDER</code>
     */
    private String createStatement;

    public TableMeta(String name, int version, String createStatement) {
        if (name.contains(";")) {
            throw new IllegalArgumentException("Don't try to hijack me boy");
        }
        this.name = name;
        this.version = version;
        this.createStatement = createStatement;
    }

    public String getName() {
        return name;
    }

    public int getVersion() {
        return version;
    }

    public String getCreateStatement() {
        return createStatement;
    }
}
