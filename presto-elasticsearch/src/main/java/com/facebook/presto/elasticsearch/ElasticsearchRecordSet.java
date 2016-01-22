package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordSet
        implements RecordSet
{
    private final List<ElasticsearchColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    //private final ByteSource byteSource;
    private final ElasticsearchTableSource tableSource;

    public ElasticsearchRecordSet(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ElasticsearchColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        /*
        try {
            byteSource = Resources.asByteSource(split.getUri().toURL());
        }
        catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }
        */
        tableSource = split.getUri();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ElasticsearchRecordCursor(columnHandles, tableSource);
    }
}
