package ru.yandex.clickhouse.cccp;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Cluster client for test purposes
 *
 * Puts data to cluster
 *
 * Created by Jkee on 05.11.2016.
 */
public class ClusterClient {

    private ZooKeeper zk;

    private String dataset;
    private String table;

    public ClusterClient(String host, int port) {
        try {
            // no listener for now
            zk = new ZooKeeper(host + ':' + port, 500, event -> {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
