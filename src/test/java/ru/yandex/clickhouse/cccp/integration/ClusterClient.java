package ru.yandex.clickhouse.cccp.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.zookeeper.ZooKeeper;
import ru.yandex.clickhouse.cccp.api.DatasetService;
import ru.yandex.clickhouse.cccp.cluster.Region;
import ru.yandex.clickhouse.cccp.index.IndexRange;
import ru.yandex.clickhouse.cccp.util.CHNodeConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Cluster client for test purposes
 *
 * Puts data to cluster.
 *
 *
 * So, we want to put data for some index range.
 *
 * Should work as follows:
 *
 * 1. Get regions for desired index range
 * 2. Register itself as writer
 * 3. Get nodes for regions
 * 4. Subscribe to nodes and update nodes structure
 * 5. Subscribe to region state to got 'closing' or 'splitting' events
 *
 *
 * In the responsibility of client:
 * 1. Split data according to index ranges
 * 2. Update nodes info
 * 3. Update regions info
 *
 * Created by Jkee on 05.11.2016.
 */
public class ClusterClient {

    private ZooKeeper zk;
    private DatasetService datasetService;

    String dataset;
    String table;
    private int globalPort = 8123;

    private Random random = new Random("Full Metal Alchemist".hashCode());

    private static class HitIndexRange {
        private TestHitIndex start;
        private TestHitIndex end;
        private int regionID;
    }

    private List<HitIndexRange> indexes = Lists.newArrayList();

    public ClusterClient(String host, int port, DatasetService datasetService) {
        this.datasetService = datasetService;
        try {
            // no listener for now
            zk = new ZooKeeper(host + ':' + port, 500, event -> {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertData(List<TestHit> hits) {
        // by region id
        Multimap<Integer, TestHit> splitted = splitDataByIndex(hits);
        for (Integer regionID : splitted.keys()) {
            Collection<TestHit> values = splitted.get(regionID);
            insertDataToRegion(regionID, values);
        }
    }

    private int getRegionForIndex(TestHitIndex index) {
        // stupid
        for (HitIndexRange hitIndexRange : indexes) {
            if (
                    hitIndexRange.start.compareTo(index) <= 0 &&
                    hitIndexRange.end.compareTo(index) > 0) {
                // found
                return hitIndexRange.regionID;
            }
        }
        // no index range found, should request one
        return requestAndRegisterRegion(index);
    }

    private int requestAndRegisterRegion(TestHitIndex index) {
        Region region = requestRegion(index);
        IndexRange indexRange = region.getIndexRange();

        List<Object> lowerBoundsExternal = indexRange.getLowerBoundsExternal(datasetService.getIndexConfig());
        TestHitIndex lower = new TestHitIndex(
                (LocalDate) lowerBoundsExternal.get(0),
                (Integer) lowerBoundsExternal.get(1),
                (Long) lowerBoundsExternal.get(2)
        );

        List<Object> upperBoundsExternal = indexRange.getUpperBoundsExternal(datasetService.getIndexConfig());
        TestHitIndex upper = new TestHitIndex(
                (LocalDate) upperBoundsExternal.get(0),
                (Integer) upperBoundsExternal.get(1),
                (Long) upperBoundsExternal.get(2)
        );

        HitIndexRange range = new HitIndexRange();
        range.regionID = region.getRegionID();
        range.start = upper;
        range.end = lower;

        indexes.add(range);

        return region.getRegionID();

    }

    private Region requestRegion(TestHitIndex index) {
        List<Object> objectIndex = Lists.newArrayList();
        objectIndex.add(index.getEventMonth());
        objectIndex.add(index.getIndexPrefix());
        objectIndex.add(index.getUserIDHash());
        return datasetService.getRegion(objectIndex);
    }

    private List<String> requestRegionHosts(int regionID) {
        return datasetService.getHosts(regionID);
    }

    private Multimap<Integer, TestHit> splitDataByIndex(List<TestHit> hits) {
        return Multimaps.index(hits, hit -> {
            // get index for hit
            TestHitIndex index = hit.toIndex();
            // search region for that index
            return getRegionForIndex(index);
        });
    }

    private void insertDataToRegion(int regionID, Collection<TestHit> hits) {
        List<String> hosts = requestRegionHosts(regionID);
        // pick random
        String host = hosts.get(random.nextInt(hosts.size()));
        // actually insert data
        try {
            String tableName = dataset + '.' + table + '_' + regionID;
            String insertQuery = "INSERT INTO " + tableName +
                    " (EventTime, IndexPrefix, UserID, UsefulData) VALUES (?, ?, ?, ?)";

            ClickHouseProperties properties = new ClickHouseProperties();
            properties.setConnectionTimeout(500);
            CHNodeConnection connection = new CHNodeConnection(host, globalPort, properties);
            PreparedStatement statement = connection.getConnection().prepareStatement(insertQuery);
            for (TestHit hit : hits) {
                statement.setTimestamp(1, Timestamp.valueOf(hit.getEventTime()));
                statement.setInt(2, hit.getIndexPrefix());
                statement.setLong(3, hit.getUserID());
                statement.setString(4, hit.getUsefulData());
                statement.addBatch();
            }
            statement.executeBatch();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
