package ru.yandex.clickhouse.cccp.cluster;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.UnsignedLongs;
import ru.yandex.clickhouse.cccp.api.DatasetService;
import ru.yandex.clickhouse.cccp.index.IndexConfig;
import ru.yandex.clickhouse.cccp.index.IndexRange;
import ru.yandex.clickhouse.cccp.index.IndexType;
import ru.yandex.clickhouse.cccp.util.ClusterDBService;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Cluster entry point
 * Created by Jkee on 29.10.2016.
 */
public class Dataset implements DatasetService {

    private Cluster cluster;

    private DatasetConfiguration configuration;

    private ClusterDBService clusterDBService;

    private List<Region> regions;

    private Random random = new Random(System.currentTimeMillis());

    /*
    * Index is presented as a tree of bounds
    *
    * */
    static class IndexBound implements Comparable<IndexBound> {

        long[] leftBound;

        public IndexBound(long[] leftBound) {
            this.leftBound = leftBound;
        }

        @Override
        public int compareTo(IndexBound o) {
            Preconditions.checkArgument(leftBound.length == o.leftBound.length);
            int compare = 0;
            for (int i = 0; i < leftBound.length; i++) {
                compare = UnsignedLongs.compare(leftBound[i], o.leftBound[i]);
                if (compare != 0) return compare;
            }
            return compare;
        }
    }

    private TreeMap<IndexBound, Region> regionIndex;

    public void initFromService(String datasetName) {
        configuration = clusterDBService.loadConfiguration(datasetName);
        regions = clusterDBService.loadRegions(configuration.getDatasetName());
        if (regions == null || regions.isEmpty()) {
            // handle new regions right
            List<IndexType<?>> types = configuration.getConfig().getTypes();
            int size = types.size();
            long[] upper = new long[size];
            long[] lower = new long[size];
            for (int i = 0; i < size; i++) {
                upper[i] = types.get(i).minValue();
                lower[i] = types.get(i).maxValue();
            }
            addRegion(upper, lower);
        }
        rebuildIndex();
    }

    private void addRegion(long[] upper, long[] lower) {
        IndexRange range = new IndexRange(upper, lower);
        List<ClusterNode> choosedNodes = chooseNodes();
        Region region = new Region(
                1,
                choosedNodes,
                range);
        regions = Lists.newArrayList();
        regions.add(region);
    }

    private List<ClusterNode> chooseNodes() {
        List<ClusterNode> choosedNodes = Lists.newArrayList();
        // choosing random <replication_factor> nodes from different datacenters
        // todo add different policies
        // todo prioritize less loaded nodes
        // todo some cool node-choosing algorithm
        Multimap<String, ClusterNode> byDC = Multimaps.index(cluster.getNodes(), ClusterNode::getDatacenter);
        int replicationFactor = configuration.getReplicationFactor();
        if (replicationFactor > byDC.size()) {
            throw new IllegalStateException("Not enough DCs for replication factor: " + replicationFactor);
        }
        List<String> dcs = Lists.newArrayList(byDC.keySet());
        Collections.shuffle(dcs, random);
        for (int i = 0; i < replicationFactor; i++) {
            String dc = dcs.get(i);
            List<ClusterNode> dcNodes = Lists.newArrayList(byDC.get(dc));
            ClusterNode node = dcNodes.get(random.nextInt(dcNodes.size()));
            choosedNodes.add(node);
        }
        return choosedNodes;
    }

    @Override
    public String getName() {
        return configuration.getDatasetName();
    }

    @Override
    public IndexConfig getIndexConfig() {
        return configuration.getConfig();
    }

    @Override
    public List<String> getHosts(int regionID) {
        for (Region region : regions) {
            if (region.getRegionID() == regionID) {
                return region.getActiveNodes().stream().map(ClusterNode::getHost).collect(Collectors.toList());
            }
        }
        throw new IllegalArgumentException("No region for id: " + regionID);
    }

    @Override
    public Region getRegion(List<Object> index) {
        List<IndexType<?>> types = configuration.getConfig().getTypes();
        Preconditions.checkArgument(index.size() == types.size(), "Index size mismatch");

        // to internal int index
        long[] longIndex = new long[index.size()];
        for (int i = 0; i < index.size(); i++) {
            Object o = index.get(i);
            IndexType type = types.get(i);
            Class<?> clazz = type.getClazz();
            Preconditions.checkArgument(clazz.equals(o.getClass()), "Index class mismatch");
            long iLongIndex = type.toBound(o);
            longIndex[i] = iLongIndex;
        }
        IndexBound indexBound = new IndexBound(longIndex);

        // lookup
        Map.Entry<IndexBound, Region> floorRegion = regionIndex.floorEntry(indexBound);
        if (floorRegion == null) {
            throw new IllegalStateException("That's not possible, floor region always should be in the index");
        }
        return floorRegion.getValue();
    }

    private void rebuildIndex() {
        TreeMap<IndexBound, Region> newIndex = new TreeMap<>();
        for (Region region : regions) {
            newIndex.put(
                    new IndexBound(region.getIndexRange().getUpperBounds()),
                    region
            );
        }
        regionIndex = newIndex;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public DatasetConfiguration getConfiguration() {
        return configuration;
    }

    public void setClusterDBService(ClusterDBService clusterDBService) {
        this.clusterDBService = clusterDBService;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public void setRegions(List<Region> regions) {
        this.regions = regions;
    }
}
