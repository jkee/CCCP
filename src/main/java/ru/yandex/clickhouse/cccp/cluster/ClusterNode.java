package ru.yandex.clickhouse.cccp.cluster;

/**
 * Just a one node, defined by host
 * Created by Jkee on 29.10.2016.
 */
public class ClusterNode {

    private String datacenter; // datacenter key
    private String host; // can be IP but mostly fqdn

    public ClusterNode() {
    }

    public ClusterNode(String datacenter, String host) {
        this.datacenter = datacenter;
        this.host = host;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public String getHost() {
        return host;
    }

    public void setDatacenter(String datacenter) {
        this.datacenter = datacenter;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterNode node = (ClusterNode) o;

        return host != null ? host.equals(node.host) : node.host == null;

    }

    @Override
    public int hashCode() {
        return host != null ? host.hashCode() : 0;
    }
}
