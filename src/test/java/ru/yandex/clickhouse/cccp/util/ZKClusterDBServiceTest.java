package ru.yandex.clickhouse.cccp.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.clickhouse.cccp.cluster.ClusterNode;
import ru.yandex.clickhouse.cccp.cluster.DatasetConfiguration;
import ru.yandex.clickhouse.cccp.dataset.TableMeta;
import ru.yandex.clickhouse.cccp.index.IndexConfig;
import ru.yandex.clickhouse.cccp.index.IndexTypes;

import java.util.HashSet;
import java.util.List;

/**
 * Some ZK tests
 * Require zookeeper
 * Created by Jkee on 31.10.2016.
 */
public class ZKClusterDBServiceTest {

    static final Logger logger = Logger.getLogger(ZKClusterDBServiceTest.class);

    private ZooKeeper zk;

    ZKClusterDBService clusterDBService;

    String clusterName = "fruits";
    String datasetName = "ananas";

    @Before
    public void setUp() throws Exception {
        BasicConfigurator.configure();

        zk = new ZooKeeper("jkee.org:2181",500, null);
        if (zk.exists("/" + clusterName, false) != null) {
            ZKUtil.deleteRecursive(zk, "/" + clusterName);
        }
        clusterDBService = new ZKClusterDBService("jkee.org", 2181, clusterName);
    }

    @After
    public void tearDown() throws Exception {
        ZKUtil.deleteRecursive(zk, "/" + clusterName);
    }

    @Test
    public void createCluster() throws Exception {
        clusterDBService.initCluster();

        Assert.assertNotNull(zk.exists('/' + clusterName, null));
        Assert.assertNotNull(zk.exists('/' + clusterName + "/nodes", null));
        Assert.assertNotNull(zk.exists('/' + clusterName + "/parameters", null));
        Assert.assertNotNull(zk.exists('/' + clusterName + "/datasets", null));

        Assert.assertTrue(clusterDBService.getDatasets().isEmpty());
    }

    @Test
    public void saveNodes() throws Exception {
        clusterDBService.initCluster();
        HashSet<ClusterNode> clusterNodes = Sets.newHashSet(
                new ClusterNode("redhead", "ananas01"),
                new ClusterNode("redhead", "ananas02"),
                new ClusterNode("redhead", "ananas03"),
                new ClusterNode("ginger", "ananas04"),
                new ClusterNode("ginger", "ananas05"),
                new ClusterNode("ginger", "ananas06"),
                new ClusterNode("bald", "ananas07"),
                new ClusterNode("bald", "ananas08"),
                new ClusterNode("bald", "ananas09")
        );
        clusterDBService.saveNodes(clusterNodes);
        Assert.assertEquals(clusterNodes, clusterDBService.getNodes());
    }

    @Test
    public void createDataset() throws Exception {
        clusterDBService.initCluster();

        Assert.assertTrue(clusterDBService.getDatasets().isEmpty());
    }

    @Test
    public void tables() throws Exception {
        clusterDBService.initCluster();

        DatasetConfiguration configuration = new DatasetConfiguration();
        configuration.setClusterName(clusterName);
        configuration.setDatasetName(datasetName);
        configuration.setReplicationFactor(3);
        configuration.setMaxTabletSize(100 * 1024); // 100 gb
        IndexConfig config = new IndexConfig(Lists.newArrayList(
                IndexTypes.MONTH,
                IndexTypes.UInt32,
                IndexTypes.UInt64
        ));
        configuration.setConfig(config);

        clusterDBService.setConfiguration(configuration);

        Assert.assertTrue(clusterDBService.getDatasets().size() == 1);

        List<TableMeta> metas = clusterDBService.loadTables(datasetName);
        Assert.assertTrue(metas.isEmpty());

        String tableName = "testtable";
        String createStatement = "CREATE BLAH BLAH";
        TableMeta tableMeta = new TableMeta(tableName, 2, createStatement);
        clusterDBService.setTable(datasetName, tableMeta);

        metas = clusterDBService.loadTables(datasetName);
        Assert.assertTrue(metas.size() == 1);

        Assert.assertEquals(tableName, metas.get(0).getName());
        Assert.assertEquals(2, metas.get(0).getVersion());
        Assert.assertEquals(createStatement, metas.get(0).getCreateStatement());

        tableMeta = new TableMeta(tableName, 3, createStatement + " BLAH");
        clusterDBService.setTable(datasetName, tableMeta);

        metas = clusterDBService.loadTables(datasetName);
        Assert.assertTrue(metas.size() == 1);

        Assert.assertEquals(tableName, metas.get(0).getName());
        Assert.assertEquals(3, metas.get(0).getVersion());
        Assert.assertEquals(createStatement + " BLAH", metas.get(0).getCreateStatement());

    }

    @Test
    public void saveAndLoad() throws Exception {

        clusterDBService.initCluster();

        DatasetConfiguration configuration = new DatasetConfiguration();
        configuration.setClusterName(clusterName);
        configuration.setDatasetName(datasetName);
        configuration.setReplicationFactor(3);
        configuration.setMaxTabletSize(100 * 1024); // 100 gb
        IndexConfig config = new IndexConfig(Lists.newArrayList(
                IndexTypes.MONTH,
                IndexTypes.UInt32,
                IndexTypes.UInt64
        ));
        configuration.setConfig(config);

        clusterDBService.setConfiguration(configuration);
        DatasetConfiguration loaded = clusterDBService.loadConfiguration(datasetName);

        Assert.assertEquals(configuration.getClusterName(), loaded.getClusterName());
        Assert.assertEquals(configuration.getDatasetName(), loaded.getDatasetName());
        Assert.assertEquals(configuration.getMaxTabletSize(), loaded.getMaxTabletSize());
        Assert.assertEquals(configuration.getReplicationFactor(), loaded.getReplicationFactor());

        Assert.assertEquals(config.getTypes(), configuration.getConfig().getTypes());

        // reload

        configuration.setReplicationFactor(4);

        clusterDBService.setConfiguration(configuration);
        loaded = clusterDBService.loadConfiguration(datasetName);

        Assert.assertEquals(configuration.getClusterName(), loaded.getClusterName());
        Assert.assertEquals(configuration.getDatasetName(), loaded.getDatasetName());
        Assert.assertEquals(configuration.getMaxTabletSize(), loaded.getMaxTabletSize());
        Assert.assertEquals(configuration.getReplicationFactor(), loaded.getReplicationFactor());

        Assert.assertEquals(config.getTypes(), configuration.getConfig().getTypes());

    }

}