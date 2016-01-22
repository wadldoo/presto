package com.facebook.presto.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class ElasticsearchTableSource
{
    private String hostaddress;
    private int port;
    private String clusterName;
    private String index;
    private String type;

    @JsonCreator
    public ElasticsearchTableSource(
            @JsonProperty("hostaddress") String hostaddress,
            @JsonProperty("port") int port,
            @JsonProperty("clusterName") String clusterName,
            @JsonProperty("index") String index,
            @JsonProperty("type") String type)
    {
        this.hostaddress = requireNonNull(hostaddress, "hostaddress is null");
        this.port = requireNonNull(port, "port is null");
        this.clusterName = requireNonNull(clusterName, "clusterName is null");
        this.index = index;
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getHostaddress()
    {
        return hostaddress;
    }

    @JsonProperty
    public int getPort()
    {
        return port;
    }

    @JsonProperty
    public String getClusterName()
    {
        return clusterName;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }
}
