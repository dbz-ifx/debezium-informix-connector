/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class InformixConnectorTask extends BaseSourceTask<InformixPartition, InformixOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixConnectorTask.class);

    private static final String CONTEXT_NAME = "informix-server-connector-task";

    private volatile InformixTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile InformixConnection dataConnection;
    private volatile InformixConnection metadataConnection;
    private volatile ErrorHandler errorHandler;
    private volatile InformixDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected ChangeEventSourceCoordinator<InformixPartition, InformixOffsetContext> start(Configuration config) {
        final InformixConnectorConfig connectorConfig = new InformixConnectorConfig(applyFetchSizeToJdbcConfig(config));
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjustmentMode().createAdjuster();

        dataConnection = new InformixConnection(connectorConfig.getJdbcConfig());
        metadataConnection = new InformixConnection(connectorConfig.getJdbcConfig());

        try {
            dataConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }

        final JdbcValueConverters valueConverters = new JdbcValueConverters(connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode(), ZoneOffset.UTC, null, null, connectorConfig.binaryHandlingMode());
        this.schema = new InformixDatabaseSchema(connectorConfig, valueConverters, schemaNameAdjuster, topicNamingStrategy, dataConnection);
        this.schema.initializeStorage();

        final Offsets<InformixPartition, InformixOffsetContext> previousOffsets = getPreviousOffsets(new InformixPartition.Provider(connectorConfig), new InformixOffsetContext.Loader(connectorConfig));
        final InformixPartition partition = previousOffsets.getTheOnlyPartition();
        final InformixOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        if (previousOffset != null) {
            schema.recover(partition, previousOffset);
        }

        taskContext = new InformixTaskContext(connectorConfig, schema);

        final Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new ErrorHandler(InformixConnector.class, connectorConfig, queue);

        final InformixEventMetadataProvider metadataProvider = new InformixEventMetadataProvider();

        final EventDispatcher<InformixPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        ChangeEventSourceCoordinator<InformixPartition, InformixOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                InformixConnector.class,
                connectorConfig,
                new InformixChangeEventSourceFactory(connectorConfig, dataConnection, metadataConnection, errorHandler, dispatcher, clock, schema),
                new DefaultChangeEventSourceMetricsFactory<>(),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        try {
            if (dataConnection != null) {
                if (dataConnection.isConnected()) {
                    try {
                        dataConnection.rollback();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
                dataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (metadataConnection != null) {
                metadataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC metadata connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return InformixConnectorConfig.ALL_FIELDS;
    }

    /**
     * Applies the fetch size to the driver/jdbc configuration from the connector configuration.
     *
     * @param config the connector configuration
     * @return the potentially modified configuration, never null
     */
    private static Configuration applyFetchSizeToJdbcConfig(Configuration config) {
        // By default, do not load whole result sets into memory
        if (config.getInteger(InformixConnectorConfig.QUERY_FETCH_SIZE) > 0) {
            final String driverPrefix = CommonConnectorConfig.DRIVER_CONFIG_PREFIX;
            return config.edit()
                    .withDefault(driverPrefix + "responseBuffering", "adaptive")
                    .withDefault(driverPrefix + "fetchSize", config.getInteger(InformixConnectorConfig.QUERY_FETCH_SIZE))
                    .build();
        }
        return config;
    }
}
