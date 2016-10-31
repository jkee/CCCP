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

        ClusterNode that = (ClusterNode) o;

        if (datacenter != null ? !datacenter.equals(that.datacenter) : that.datacenter != null) return false;
        return host != null ? host.equals(that.host) : that.host == null;
    }

    @Override
    public int hashCode() {
        int result = datacenter != null ? datacenter.hashCode() : 0;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        return result;
    }
}
