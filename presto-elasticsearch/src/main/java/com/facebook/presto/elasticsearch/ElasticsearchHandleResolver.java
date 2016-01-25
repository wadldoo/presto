package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ElasticsearchHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return ElasticsearchTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return null;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return ElasticsearchColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return ElasticsearchSplit.class;
    }

    static ElasticsearchTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof ElasticsearchTableHandle, "tableHandle is not an instance of ElasticsearchTableHandle");
        return (ElasticsearchTableHandle) tableHandle;
    }

    static ElasticsearchColumnHandle convertColumnHandle(ColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof ElasticsearchColumnHandle, "columnHandle is not an instance of ElasticsearchColumnHandle");
        return (ElasticsearchColumnHandle) columnHandle;
    }

    static ElasticsearchSplit convertSplit(ConnectorSplit split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof ElasticsearchSplit, "split is not an instance of ElasticsearchSplit");
        return (ElasticsearchSplit) split;
    }

    static ElasticsearchTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout)
    {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof ElasticsearchTableLayoutHandle, "layout is not an instance of ElasticsearchTableLayoutHandle");
        return (ElasticsearchTableLayoutHandle) layout;
    }
}
