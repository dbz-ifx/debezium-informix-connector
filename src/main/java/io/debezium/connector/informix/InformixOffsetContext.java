/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import static io.debezium.connector.informix.SourceInfo.BEGIN_LSN_KEY;
import static io.debezium.connector.informix.SourceInfo.CHANGE_LSN_KEY;
import static io.debezium.connector.informix.SourceInfo.COMMIT_LSN_KEY;
import static io.debezium.connector.informix.SourceInfo.SNAPSHOT_KEY;
import static java.lang.Boolean.TRUE;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

public class InformixOffsetContext extends CommonOffsetContext<SourceInfo> {

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    private static final String EVENT_SERIAL_NO_KEY = "event_serial_no";

    private final Schema sourceInfoSchema;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private boolean snapshotCompleted;

    public InformixOffsetContext(InformixConnectorConfig connectorConfig, TxLogPosition position, boolean snapshot, boolean snapshotCompleted,
                                 TransactionContext transactionContext, IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(connectorConfig));

        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getChangeLsn());
        sourceInfo.setBeginLsn(position.getBeginLsn());
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }

        this.transactionContext = transactionContext;

        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public InformixOffsetContext(InformixConnectorConfig connectorConfig, TxLogPosition position, boolean snapshot, boolean snapshotCompleted) {
        this(connectorConfig, position, snapshot, snapshotCompleted, new TransactionContext(), new SignalBasedIncrementalSnapshotContext<>(false));
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            return Collect.hashMapOf(SNAPSHOT_KEY, true, SNAPSHOT_COMPLETED_KEY, snapshotCompleted, COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString(), CHANGE_LSN_KEY,
                    sourceInfo.getChangeLsn() == null ? null : sourceInfo.getChangeLsn().toString(), BEGIN_LSN_KEY,
                    sourceInfo.getBeginLsn() == null ? null : sourceInfo.getBeginLsn().toString());
        }
        else {
            return incrementalSnapshotContext.store(transactionContext.store(Collect.hashMapOf(COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString(), CHANGE_LSN_KEY,
                    sourceInfo.getChangeLsn() == null ? null : sourceInfo.getChangeLsn().toString(), BEGIN_LSN_KEY,
                    sourceInfo.getBeginLsn() == null ? null : sourceInfo.getBeginLsn().toString())));
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public TxLogPosition getChangePosition() {
        return TxLogPosition.valueOf(sourceInfo.getCommitLsn(), sourceInfo.getChangeLsn(), sourceInfo.getTxId(), sourceInfo.getBeginLsn());
    }

    public void setChangePosition(TxLogPosition position) {
        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getChangeLsn());
        sourceInfo.setTxId(position.getTxId());
        sourceInfo.setBeginLsn(position.getBeginLsn());
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public String toString() {
        return "InformixOffsetContext [" + "sourceInfoSchema=" + sourceInfoSchema + ", sourceInfo=" + sourceInfo + ", snapshotCompleted=" + snapshotCompleted + "]";
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.setTableId((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public static class Loader implements OffsetContext.Loader<InformixOffsetContext> {

        private final InformixConnectorConfig connectorConfig;

        public Loader(InformixConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public InformixOffsetContext load(Map<String, ?> offset) {
            final Lsn commitLsn = Lsn.valueOf((String) offset.get(COMMIT_LSN_KEY));
            final Lsn changeLsn = Lsn.valueOf((String) offset.get(CHANGE_LSN_KEY));
            final Lsn beginLsn = Lsn.valueOf((String) offset.get(BEGIN_LSN_KEY));

            boolean snapshot = TRUE.equals(offset.get(SNAPSHOT_KEY));
            boolean snapshotCompleted = TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            return new InformixOffsetContext(connectorConfig, TxLogPosition.valueOf(commitLsn, changeLsn, beginLsn), snapshot, snapshotCompleted,
                    TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext.load(offset, false));
        }
    }

}
