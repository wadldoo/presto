package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.io.Resources;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.elasticsearch.MetadataUtil.CATALOG_CODEC;
import static org.testng.Assert.assertNotNull;

public class TestElasticsearchRecordCursor
{
    private static final String ES_SCHEMA = "be";
    private static final String ES_TBL_1 = "fancyPantsTable";
    private static final String CONNECTOR_ID = "elasticsearch";

    private ElasticsearchClient client;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        URL metadataUrl = Resources.getResource(TestElasticsearchClient.class, "/example-metadata.json");
        assertNotNull(metadataUrl, "metadataUrl is null");
        URI metadata = metadataUrl.toURI();

        client = new ElasticsearchClient(new ElasticsearchConfig().setMetadata(metadata), CATALOG_CODEC);
    }

    @Test
    public void testCursor()
    {
        ElasticsearchTable table = client.getTable(ES_SCHEMA, ES_TBL_1);
        assertNotNull(table);

        ElasticsearchRecordCursor elasticsearchRecordCursor =
                new ElasticsearchRecordCursor(
                        getElasticsearchColumnHandles(table),
                        table.getSources().get(0));

        assertNotNull(elasticsearchRecordCursor);
    }

    private List<ElasticsearchColumnHandle> getElasticsearchColumnHandles(ElasticsearchTable table)
    {
        List<ElasticsearchColumnHandle> columnHandles = new ArrayList();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            ElasticsearchColumnMetadata esColumn = (ElasticsearchColumnMetadata) column;

            columnHandles.add(new ElasticsearchColumnHandle(
                    CONNECTOR_ID,
                    column.getName(),
                    column.getType(),
                    esColumn.getJsonPath(),
                    esColumn.getJsonType(),
                    index++));
        }
        return columnHandles;
    }
}
