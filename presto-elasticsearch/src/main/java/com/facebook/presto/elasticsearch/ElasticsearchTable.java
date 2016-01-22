package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ElasticsearchTable
{
    private final String name;

    private List<ElasticsearchColumn> columns;
    private List<ElasticsearchColumnMetadata> columnsMetadata;
    private final List<ElasticsearchTableSource> sources;

    @JsonCreator
    public ElasticsearchTable(
            @JsonProperty("name") String name,
            @JsonProperty("sources") List<ElasticsearchTableSource> sources)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name.toLowerCase(ENGLISH), "name is null");
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<ElasticsearchColumn> getColumns()
    {
        return columns;
    }

    public void setColumns(List<ElasticsearchColumn> columns)
    {
        this.columns = columns;
    }

    @JsonProperty
    public List<ElasticsearchTableSource> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        //List<? extends ColumnMetadata> bases = columnsMetadata;
        //return bases;

        //List<ColumnMetadata> lis = new ArrayList<ColumnMetadata>(columnsMetadata);
        //return lis;

        return new ArrayList<>(columnsMetadata);
    }

    public void setColumnsMetadata(List<ElasticsearchColumnMetadata> columnsMetadata)
    {
        this.columnsMetadata = columnsMetadata;
    }
}
