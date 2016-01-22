package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.predicate.TupleDomain;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElasticsearchPartition
        implements ConnectorPartition
{
    private final String schemaName;
    private final String tableName;

    public ElasticsearchPartition(String schemaName, String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.tableName = requireNonNull(tableName, "table name is null");
    }

    @Override
    public String getPartitionId()
    {
        return schemaName + ":" + tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.all();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .toString();
    }
}
