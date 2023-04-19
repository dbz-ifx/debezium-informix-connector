/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import static com.informix.stream.api.IfmxStreamRecordType.COMMIT;
import static com.informix.stream.api.IfmxStreamRecordType.ROLLBACK;
import static io.debezium.data.Envelope.Operation.CREATE;
import static io.debezium.data.Envelope.Operation.DELETE;
import static io.debezium.data.Envelope.Operation.TRUNCATE;
import static io.debezium.data.Envelope.Operation.UPDATE;
import static io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType.ALTER;
import static java.lang.Thread.currentThread;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxReadableType;
import com.informix.jdbc.IfxColumnInfo;
import com.informix.stream.api.IfmxStreamOperationRecord;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.records.IfxCDCBeginTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCCommitTransactionRecord;
import com.informix.stream.cdc.records.IfxCDCMetaDataRecord;
import com.informix.stream.cdc.records.IfxCDCTruncateRecord;
import com.informix.stream.impl.IfxStreamException;

import io.debezium.data.Envelope;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.ChangeTable;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class InformixStreamingChangeEventSource implements StreamingChangeEventSource<InformixPartition, InformixOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixStreamingChangeEventSource.class);

    private final InformixConnectorConfig connectorConfig;
    private final InformixConnection dataConnection;
    private final EventDispatcher<InformixPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final InformixDatabaseSchema schema;
    private InformixOffsetContext effectiveOffsetContext;

    public InformixStreamingChangeEventSource(InformixConnectorConfig connectorConfig, InformixConnection dataConnection,
                                              EventDispatcher<InformixPartition, TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                              InformixDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
    }

    private static void updateChangePosition(InformixOffsetContext offsetContext, Long beginSeq, Long changeSeq, Long commitSeq, Integer transactionId) {
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(offsetContext.getChangePosition(), Lsn.valueOf(commitSeq), Lsn.valueOf(changeSeq), transactionId, Lsn.valueOf(beginSeq)));
    }

    @Override
    public void init(InformixOffsetContext offsetContext) {
        this.effectiveOffsetContext = offsetContext == null ? new InformixOffsetContext(connectorConfig, TxLogPosition.valueOf(Lsn.valueOf(0x00L)), false, false)
                : offsetContext;
    }

    /**
     * Executes this source. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context contextual information for this source's execution
     * @throws InterruptedException in case the snapshot was aborted before completion
     */
    @Override
    public void execute(ChangeEventSourceContext context, InformixPartition partition, InformixOffsetContext offsetContext) throws InterruptedException {
        /*
         * Initialize CDC Engine before main loop.
         */
        TxLogPosition lastPosition = offsetContext.getChangePosition();
        Lsn lastCommitLsn = lastPosition.getCommitLsn();
        Lsn lastBeginLsn = lastPosition.getBeginLsn();

        try (InformixCDCTransactionEngine transactionEngine = dataConnection.getTransactionEngine(context, schema, lastBeginLsn)) {
            transactionEngine.init();

            InformixStreamTransactionRecord transactionRecord = transactionEngine.getTransaction();
            /*
             * Recover Stage. In this stage, we replay event from 'changeLsn' to 'commitLsn', and rebuild the transactionCache.
             */
            if (lastBeginLsn.compareTo(lastCommitLsn) < 0) {
                LOGGER.info("Begin recover: from lastBeginLsn='{}' to lastCommitLsn='{}'", lastBeginLsn, lastCommitLsn);
                while (context.isRunning()) {
                    Lsn commitLsn = Lsn.valueOf(transactionRecord.getEndRecord().getSequenceId());
                    if (commitLsn.compareTo(lastCommitLsn) < 0) {
                        LOGGER.info("Skipping transaction with id: '{}' since commitLsn='{}' < lastCommitLsn='{}'", transactionRecord.getTransactionId(), commitLsn,
                                lastCommitLsn);
                    }
                    else if (commitLsn.compareTo(lastCommitLsn) > 0) {
                        LOGGER.info("Recover finished: from lastBeginLsn='{}' to lastCommitLsn='{}', current Lsn='{}'", lastBeginLsn, lastCommitLsn, commitLsn);
                        break;
                    }
                    else {
                        handleTransaction(transactionEngine, partition, offsetContext, transactionRecord, true);
                    }
                    transactionRecord = transactionEngine.getTransaction();
                }
            }
            /*
             * Main Handler Loop
             */
            while (context.isRunning()) {
                handleTransaction(transactionEngine, partition, offsetContext, transactionRecord, false);
                transactionRecord = transactionEngine.getTransaction();
            }
        }
        catch (InterruptedException e) {
            LOGGER.error("Caught InterruptedException", e);
            errorHandler.setProducerThrowable(e);
            currentThread().interrupt();
        }
        catch (Exception e) {
            LOGGER.error("Caught Exception", e);
            errorHandler.setProducerThrowable(e);
        }
    }

    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        // NOOP
    }

    @Override
    public InformixOffsetContext getOffsetContext() {
        return effectiveOffsetContext;
    }

    private void handleTransaction(InformixCDCTransactionEngine engine, InformixPartition partition, InformixOffsetContext offsetContext,
                                   InformixStreamTransactionRecord transactionRecord, boolean recover)
            throws InterruptedException, IfxStreamException {
        long tStart = System.nanoTime();

        int transactionId = transactionRecord.getTransactionId();

        IfxCDCBeginTransactionRecord beginRecord = transactionRecord.getBeginRecord();
        IfmxStreamRecord endRecord = transactionRecord.getEndRecord();

        long start = System.nanoTime();

        long beginTs = beginRecord.getTime();
        long beginSeq = beginRecord.getSequenceId();
        long lowestBeginSeq = engine.getLowestBeginSequence().orElse(beginSeq);
        long endSeq = endRecord.getSequenceId();

        if (!recover) {
            updateChangePosition(offsetContext, lowestBeginSeq, beginSeq, null, transactionId);
            dispatcher.dispatchTransactionStartedEvent(partition, String.valueOf(transactionId), offsetContext, Instant.ofEpochSecond(beginTs));
        }

        long end = System.nanoTime();

        LOGGER.info("Received {} Time [{}] UserId [{}] ElapsedT [{}ms]", beginRecord, beginTs, beginRecord.getUserId(), (end - start) / 1000000d);

        if (COMMIT.equals(endRecord.getType())) {
            IfxCDCCommitTransactionRecord commitRecord = (IfxCDCCommitTransactionRecord) endRecord;
            long commitSeq = commitRecord.getSequenceId();
            long commitTs = commitRecord.getTime();

            if (!recover) {
                updateChangePosition(offsetContext, null, null, commitSeq, transactionId);
            }

            Map<String, IfmxReadableType> before = null;
            Map<String, TableId> label2TableId = engine.getTableIdByLabelId();

            for (IfmxStreamRecord ifxRecord : transactionRecord.getRecords()) {
                start = System.nanoTime();

                long changeSeq = ifxRecord.getSequenceId();

                if (recover && Lsn.valueOf(changeSeq).compareTo(offsetContext.getChangePosition().getChangeLsn()) <= 0) {
                    LOGGER.info("Skipping already processed record {}", changeSeq);
                    continue;
                }

                Optional<TableId> tableId = Optional.ofNullable(ifxRecord.getLabel()).map(label2TableId::get);

                Map<String, IfmxReadableType> after;

                updateChangePosition(offsetContext, null, changeSeq, null, transactionId);

                switch (ifxRecord.getType()) {
                    case INSERT:

                        after = ((IfmxStreamOperationRecord) ifxRecord).getData();

                        handleOperation(partition, offsetContext, CREATE, null, after, tableId.orElseThrow());

                        end = System.nanoTime();

                        LOGGER.info("Received {} ElapsedT [{}ms] Data After [{}]", ifxRecord, (end - start) / 1000000d, after);
                        break;
                    case BEFORE_UPDATE:

                        before = ((IfmxStreamOperationRecord) ifxRecord).getData();

                        end = System.nanoTime();

                        LOGGER.info("Received {} ElapsedT [{}ms] Data Before [{}]", ifxRecord, (end - start) / 1000000d, before);
                        break;
                    case AFTER_UPDATE:

                        after = ((IfmxStreamOperationRecord) ifxRecord).getData();

                        handleOperation(partition, offsetContext, UPDATE, before, after, tableId.orElseThrow());

                        end = System.nanoTime();

                        LOGGER.info("Received {} ElapsedT [{}ms] Data Before [{}] Data After [{}]", ifxRecord, (end - start) / 1000000d, before, after);
                        break;
                    case DELETE:

                        before = ((IfmxStreamOperationRecord) ifxRecord).getData();

                        handleOperation(partition, offsetContext, DELETE, before, null, tableId.orElseThrow());

                        end = System.nanoTime();

                        LOGGER.info("Received {} ElapsedT [{}ms] Data Before [{}]", ifxRecord, (end - start) / 1000000d, before);
                        break;
                    case TRUNCATE:
                        /*
                         * According to IBM documentation the 'User data' field of the CDC_REC_TRUNCATE record header contains the
                         * table identifier, otherwise placed in the IfxCDCRecord 'label' field. For unknown reasons, this is
                         * instead placed in the 'userId' field?
                         */
                        IfxCDCTruncateRecord truncateRecord = (IfxCDCTruncateRecord) ifxRecord;
                        tableId = Optional.of(truncateRecord.getUserId()).map(Number::toString).map(label2TableId::get);

                        handleOperation(partition, offsetContext, TRUNCATE, null, null, tableId.orElseThrow());

                        LOGGER.info("Received {} ElapsedT [{}ms]", ifxRecord, (end - start) / 1000000d);
                        break;
                    case METADATA:
                        IfxCDCMetaDataRecord metaDataRecord = (IfxCDCMetaDataRecord) ifxRecord;
                        List<IfxColumnInfo> columns = metaDataRecord.getColumns();

                        ChangeTable changeTable = new ChangeTable(tableId.orElseThrow().table(), tableId.orElseThrow(), null,
                                Integer.parseInt(metaDataRecord.getLabel()));

                        handleMetadata(partition, offsetContext, changeTable);

                        LOGGER.info("Received {} ElapsedT [{}ms] Columns [{}]", ifxRecord, (end - start) / 1000000d,
                                columns.stream().map(IfxColumnInfo::getColumnName).toArray());
                        break;
                    case TIMEOUT:
                    case ERROR:
                        end = System.nanoTime();

                        LOGGER.info("Received {} ElapsedT [{}ms]", ifxRecord, (end - start) / 1000000d);
                        break;
                    default:
                        end = System.nanoTime();

                        LOGGER.info("Handle unknown record-type {} ElapsedT [{}ms]", ifxRecord, (end - start) / 1000000d);
                }
            }

            start = System.nanoTime();

            updateChangePosition(offsetContext, null, commitSeq, null, transactionId);
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, Instant.ofEpochSecond(commitTs));

            end = System.nanoTime();

            LOGGER.info("Received {} Time [{}] UserId [{}] ElapsedT [{}ms]", endRecord, commitTs, beginRecord.getUserId(), (end - start) / 1000000d);

            LOGGER.info("Handle Transaction Events [{}], ElapsedT [{}]", transactionRecord.getRecords().size(), (end - tStart) / 1000000d);
        }
        if (ROLLBACK.equals(endRecord.getType())) {

            if (!recover) {
                updateChangePosition(offsetContext, null, endSeq, endSeq, transactionId);
                offsetContext.getTransactionContext().endTransaction();
            }

            end = System.nanoTime();

            LOGGER.info("Received {} ElapsedT [{}ms]", endRecord, (end - start) / 1000000d);
        }
    }

    private void handleOperation(InformixPartition partition, InformixOffsetContext offsetContext, Envelope.Operation operation, Map<String, IfmxReadableType> before,
                                 Map<String, IfmxReadableType> after, TableId tableId)
            throws InterruptedException {
        offsetContext.event(tableId, clock.currentTime());

        dispatcher.dispatchDataChangeEvent(partition, tableId, new InformixChangeRecordEmitter(partition, offsetContext, clock, operation,
                InformixChangeRecordEmitter.convertIfxData2Array(before), InformixChangeRecordEmitter.convertIfxData2Array(after), connectorConfig));
    }

    private void handleMetadata(InformixPartition partition, InformixOffsetContext offsetContext, ChangeTable changeTable) throws InterruptedException {
        offsetContext.event(changeTable.getSourceTableId(), clock.currentTime());

        dispatcher.dispatchSchemaChangeEvent(partition, offsetContext, changeTable.getSourceTableId(),
                new InformixSchemaChangeEventEmitter(partition, offsetContext, changeTable, schema.tableFor(changeTable.getSourceTableId()), schema, ALTER));
    }

}
