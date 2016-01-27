package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ElasticsearchConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;

    public ElasticsearchConnectorFactory(TypeManager typeManager, Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "elasticsearch";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ElasticsearchHandleResolver();
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        requireNonNull(optionalConfig, "optionalConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new ElasticsearchModule(connectorId, typeManager));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(ElasticsearchConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
