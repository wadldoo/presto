package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Created by lucianmatei on 25/01/16.
 */
public class ElasticsearchTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ElasticsearchTableHandle table;

    @JsonCreator
    public ElasticsearchTableLayoutHandle(@JsonProperty("table") ElasticsearchTableHandle table)
    {
        this.table = requireNonNull(table, "table is null");
    }

    @JsonProperty
    public ElasticsearchTableHandle getTable()
    {
        return table;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
