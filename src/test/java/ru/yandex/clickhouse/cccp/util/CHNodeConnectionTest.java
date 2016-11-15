package ru.yandex.clickhouse.cccp.util;

import com.google.common.collect.Lists;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.clickhouse.cccp.dataset.DatasetMeta;
import ru.yandex.clickhouse.cccp.dataset.TableMeta;
import ru.yandex.clickhouse.cccp.index.IndexConfig;
import ru.yandex.clickhouse.cccp.index.IndexTypes;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * Some ClickHouse integrations
 * Created by Jkee on 05.11.2016.
 */
public class CHNodeConnectionTest {

    static final Logger logger = Logger.getLogger(CHNodeConnectionTest.class);

    CHNodeConnection connection;
    private IndexConfig config;
    private TableMeta table;
    private DatasetMeta dataset;

    private Connection dbConnection;

    @Before
    public void setUp() throws Exception {
        BasicConfigurator.configure();

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setConnectionTimeout(500);

        connection = new CHNodeConnection("jkee.org", 8123, properties);
        dbConnection = connection.getConnection();

        config = new IndexConfig(Lists.newArrayList(
                IndexTypes.MONTH, IndexTypes.UInt64
        ));

        table = new TableMeta("hits", 0,
                "CREATE TABLE IF NOT EXISTS " + TableMeta.TABLE_PLACEHOLDER +
                        "(" +
                        "    EventDate Date," +
                        "    UserID UInt64," +
                        "    UsefulField String" +
                        ") ENGINE = MergeTree(EventDate, intHash32(UserID), (EventDate, intHash32(UserID)), 8192)"
        );

        dataset = new DatasetMeta("testdataset", config, Lists.newArrayList(table));


        dbConnection.createStatement().execute("drop database if exists testdataset");
    }

    @After
    public void tearDown() throws Exception {
        dbConnection.createStatement().execute("drop database if exists testdataset");
    }

    @Test
    public void createDataset() throws Exception {
        connection.createDataset(dataset);

        Statement statement = dbConnection.createStatement();
        statement.execute("show databases");
        ResultSet resultSet = statement.getResultSet();
        Set<String> dbs = new HashSet<>();
        while (resultSet.next()) {
            dbs.add(resultSet.getString("name"));
        }
        Assert.assertTrue("test database wasn't created", dbs.contains("testdataset"));
    }


    @Test
    public void createTables() throws Exception {

        connection.createDataset(dataset);
        connection.createTables(dataset, 42);

        Statement statement = dbConnection.createStatement();
        statement.execute("show tables from testdataset");
        ResultSet resultSet = statement.getResultSet();
        Set<String> dbs = new HashSet<>();
        while (resultSet.next()) {
            dbs.add(resultSet.getString("name"));
        }
        Assert.assertTrue("test table wasn't created", dbs.contains("hits_42"));
    }
}