package ru.yandex.clickhouse.cccp.util;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Created by Jkee on 29.10.2016.
 */
public class ZKUtils {

    public static void createIfNotExists(ZooKeeper zk, String path) {
        try {
            if (zk.exists(path, false) == null) {
                zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw new RuntimeException(e);
            } // else it's just OK
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateString(ZooKeeper zk, String path, String value) {
        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData(path, value.getBytes(), stat.getVersion());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
