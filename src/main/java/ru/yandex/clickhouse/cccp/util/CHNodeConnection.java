package ru.yandex.clickhouse.cccp.util;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.cccp.dataset.Dataset;
import ru.yandex.clickhouse.cccp.dataset.Table;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Connection to one node
 *
 * Responsible for:
 * * Creating cluster dataset (e.g. database)
 * * Creating tables for region
 * * Drop tables for region
 *
 * todo: copying data from other region
 *
 * Created by Jkee on 04.11.2016.
 */
public class CHNodeConnection {

    private String host;
    private int port;

    private ClickHouseProperties properties;
    private ClickHouseDataSource dataSource;
    private Connection connection;

    public CHNodeConnection(String host, int port, ClickHouseProperties properties) {
        this.host = host;
        this.port = port;
        this.properties = properties;
        String connectionString = "jdbc:clickhouse://" + host + ":" + port;
        dataSource = new ClickHouseDataSource(connectionString, properties);
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createDataset(Dataset dataset) {
        try {
            // hope nobody will sqlinject this shit
            // can't escape table name with jdbc things
            Statement statement = connection.createStatement();
            statement.execute("CREATE DATABASE IF NOT EXISTS " + dataset.getName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTables(Dataset dataset, int regionID) {
        try {
            for (Table table : dataset.getTables()) {
                String tableName = dataset.getName() + '.' + table.getName() + '_' + regionID;

                String sql = table.getCreateStatement().replace(Table.TABLE_PLACEHOLDER, tableName);

                Statement statement = connection.createStatement();
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ClickHouseDataSource getDataSource() {
        return dataSource;
    }

    public Connection getConnection() {
        return connection;
    }
}
