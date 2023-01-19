/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import java.util.Optional;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public class InformixChangeEventSourceFactory implements ChangeEventSourceFactory<InformixPartition, InformixOffsetContext> {

    private final InformixConnectorConfig configuration;
    private final InformixConnection dataConnection;
    private final InformixConnection metadataConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<InformixPartition, TableId> dispatcher;
    private final Clock clock;
    private final InformixDatabaseSchema schema;

    public InformixChangeEventSourceFactory(InformixConnectorConfig configuration, InformixConnection dataConnection, InformixConnection metadataConnection,
                                            ErrorHandler errorHandler, EventDispatcher<InformixPartition, TableId> dispatcher, Clock clock, InformixDatabaseSchema schema) {
        this.configuration = configuration;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource<InformixPartition, InformixOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return new InformixSnapshotChangeEventSource(configuration, dataConnection, schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<InformixPartition, InformixOffsetContext> getStreamingChangeEventSource() {
        return new InformixStreamingChangeEventSource(
                configuration,
                dataConnection,
                dispatcher,
                errorHandler,
                clock,
                schema);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<InformixPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                              InformixOffsetContext offsetContext,
                                                                                                                              SnapshotProgressListener<InformixPartition> snapshotProgressListener,
                                                                                                                              DataChangeEventListener<InformixPartition> dataChangeEventListener) {
        final SignalBasedIncrementalSnapshotChangeEventSource<InformixPartition, TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                configuration,
                dataConnection,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
