/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import com.informix.jdbc.IfmxReadableType;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.records.*;
import com.informix.stream.impl.IfxStreamException;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import static io.debezium.connector.informix.InformixChangeRecordEmitter.INSERT;
import static io.debezium.connector.informix.InformixChangeRecordEmitter.UPDATE;
import static io.debezium.connector.informix.InformixChangeRecordEmitter.DELETE;
import static io.debezium.connector.informix.InformixChangeRecordEmitter.TRUNCATE;

public class InformixStreamingChangeEventSource implements StreamingChangeEventSource<InformixPartition, InformixOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixStreamingChangeEventSource.class);

    private final InformixConnectorConfig config;
    private final InformixConnection dataConnection;
    private final EventDispatcher<InformixPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final InformixDatabaseSchema schema;

    public InformixStreamingChangeEventSource(InformixConnectorConfig connectorConfig,
                                              InformixConnection dataConnection,
                                              EventDispatcher<InformixPartition, TableId> dispatcher,
                                              ErrorHandler errorHandler,
                                              Clock clock,
                                              InformixDatabaseSchema schema) {
        this.config = connectorConfig;
        this.dataConnection = dataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
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
        InformixCDCEngine cdcEngine = dataConnection.getCdcEngine();
        InformixTransactionCache transCache = offsetContext.getInformixTransactionCache();

        /*
         * Initialize CDC Engine before main loop;
         */
        TxLogPosition lastPosition = offsetContext.getChangePosition();
        Long fromLsn = lastPosition.getCommitLsn();
        cdcEngine.setStartLsn(fromLsn);
        cdcEngine.init(schema);

        /*
         * Recover Stage. In this stage, we replay event from 'commitLsn' to 'changeLsn', and rebuild the transactionCache.
         */
        while (context.isRunning()) {
            if (lastPosition.getChangeLsn() <= lastPosition.getCommitLsn()) {
                LOGGER.info("Recover skipped, since changeLsn='{}' >= commitLsn='{}'",
                        lastPosition.getChangeLsn(), lastPosition.getCommitLsn());
                break;
            }

            try {
                IfmxStreamRecord streamRecord = cdcEngine.getCdcEngine().getRecord();
                if (streamRecord.getSequenceId() >= lastPosition.getChangeLsn()) {
                    LOGGER.info("Recover finished: from {} to {}, now Current seqId={}",
                            lastPosition.getCommitLsn(), lastPosition.getChangeLsn(), streamRecord.getSequenceId());
                    break;
                }
                switch (streamRecord.getType()) {
                    case TIMEOUT:
                        handleTimeout(cdcEngine, partition, offsetContext, (IfxCDCTimeoutRecord) streamRecord, transCache, true);
                        break;
                    case BEFORE_UPDATE:
                        handleBeforeUpdate(cdcEngine, partition, offsetContext, (IfxCDCOperationRecord) streamRecord, transCache, true);
                        break;
                    case AFTER_UPDATE:
                        handleAfterUpdate(cdcEngine, partition, offsetContext, (IfxCDCOperationRecord) streamRecord, transCache, true);
                        break;
                    case BEGIN:
                        handleBegin(cdcEngine, partition, offsetContext, (IfxCDCBeginTransactionRecord) streamRecord, transCache, true);
                        break;
                    case INSERT:
                        handleInsert(cdcEngine, partition, offsetContext, (IfxCDCOperationRecord) streamRecord, transCache, true);
                        break;
                    case COMMIT:
                        handleCommit(cdcEngine, partition, offsetContext, (IfxCDCCommitTransactionRecord) streamRecord, transCache, true);
                        break;
                    case ROLLBACK:
                        handleRollback(cdcEngine, partition, offsetContext, (IfxCDCRollbackTransactionRecord) streamRecord, transCache, false);
                        break;
                    case METADATA:
                        handleMetadata(cdcEngine, partition, offsetContext, (IfxCDCMetaDataRecord) streamRecord, transCache, true);
                        break;
                    case TRUNCATE:
                        handleTruncate(cdcEngine, partition, offsetContext, (IfxCDCTruncateRecord) streamRecord, transCache, true);
                        break;
                    case DELETE:
                        handleDelete(cdcEngine, partition, offsetContext, (IfxCDCOperationRecord) streamRecord, transCache, true);
                        break;
                    default:
                        LOGGER.info("Handle unknown record-type = {}", streamRecord.getType());
                }
            }
            catch (SQLException e) {
                LOGGER.error("Caught SQLException", e);
                errorHandler.setProducerThrowable(e);
            }
            catch (IfxStreamException e) {
                LOGGER.error("Caught IfxStreamException", e);
                errorHandler.setProducerThrowable(e);
            }
        }

        /*
         * Main Handler Loop
         */
        try {
            while (context.isRunning()) {
                cdcEngine.stream((IfmxStreamRecord streamRecord) -> {
                    switch (streamRecord.getType()) {
                        case TIMEOUT:
                            handleTimeout(cdcEngine, partition, offsetContext, (IfxCDCTimeoutRecord) streamRecord, transCache, false);
                            break;
                        case BEFORE_UPDATE:
                            handleBeforeUpdate(cdcEngine, partition, offsetContext, (IfxCDCOperationRecord) streamRecord, transCache, false);
                            break;
                        case AFTER_UPDATE:
                            handleAfterUpdate(cdcEngine, partition, offsetContext, (IfxCDCOperationRecord) streamRecord, transCache, false);
                            break;
                        case BEGIN:
                            handleBegin(cdcEngine, partition, offsetContext, (IfxCDCBeginTransactionRecord) streamRecord, transCache, false);
                            break;
                        case INSERT:
                            handleInsert(cdcEngine, partition, offsetContext, (IfxCDCOperationRecord) streamRecord, transCache, false);
                            break;
                        case COMMIT:
                            handleCommit(cdcEngine, partition, offsetContext, (IfxCDCCommitTransactionRecord) streamRecord, transCache, false);
                            break;
                        case ROLLBACK:
                            handleRollback(cdcEngine, partition, offsetContext, (IfxCDCRollbackTransactionRecord) streamRecord, transCache, false);
                            break;
                        case METADATA:
                            handleMetadata(cdcEngine, partition, offsetContext, (IfxCDCMetaDataRecord) streamRecord, transCache, false);
                            break;
                        case TRUNCATE:
                            handleTruncate(cdcEngine, partition, offsetContext, (IfxCDCTruncateRecord) streamRecord, transCache, false);
                            break;
                        case DELETE:
                            handleDelete(cdcEngine, partition, offsetContext, (IfxCDCOperationRecord) streamRecord, transCache, false);
                            break;
                        default:
                            LOGGER.info("Handle unknown record-type = {}", streamRecord.getType());
                    }

                    return false;
                });
            }
        }
        catch (SQLException e) {
            LOGGER.error("Caught SQLException", e);
            errorHandler.setProducerThrowable(e);
        }
        catch (IfxStreamException e) {
            LOGGER.error("Caught IfxStreamException", e);
            errorHandler.setProducerThrowable(e);
        }
        catch (Exception e) {
            LOGGER.error("Caught Unknown Exception", e);
            errorHandler.setProducerThrowable(e);
        }
        finally {
            cdcEngine.close();
        }
    }

    public void handleTimeout(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCTimeoutRecord timeoutRecord, InformixTransactionCache transactionCache, boolean recover) {
        offsetContext.setChangePosition(
                TxLogPosition.cloneAndSet(
                        offsetContext.getChangePosition(),
                        TxLogPosition.LSN_NULL,
                        timeoutRecord.getSequenceId(),
                        TxLogPosition.LSN_NULL,
                        TxLogPosition.LSN_NULL));
    }

    public void handleMetadata(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCMetaDataRecord metaDataRecord, InformixTransactionCache transactionCache, boolean recover) {


        LOGGER.info("Received A Metadata: type={}, label={}, seqId={}",
                metaDataRecord.getType(), metaDataRecord.getLabel(), metaDataRecord.getSequenceId());

        /*
         * IfxCDCEngine engine = cdcEngine.getCdcEngine();
         * List<IfxCDCEngine.IfmxWatchedTable> watchedTables = engine.getBuilder().getWatchedTables();
         * List<IfxColumnInfo> cols = record.getColumns();
         * for (IfxColumnInfo cinfo : cols) {
         * LOGGER.info("ColumnInfo: colName={}, {}", cinfo.getColumnName(), cinfo.toString());
         * }
         * 
         * for (IfxCDCEngine.IfmxWatchedTable tbl : watchedTables) {
         * LOGGER.info("Engine Watched Table: label={}, tabName={}", tbl.getLabel(), tbl.getTableName());
         * }
         */
    }

    public void handleBeforeUpdate(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCOperationRecord operationRecord, InformixTransactionCache transactionCache, boolean recover) throws IfxStreamException {

        Map<String, IfmxReadableType> dataBefore = operationRecord.getData();
        Long transId = (long) operationRecord.getTransactionId();
        transactionCache.beforeUpdate(transId, dataBefore);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            TxLogPosition.LSN_NULL,
                            operationRecord.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

    }

    public void handleAfterUpdate(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCOperationRecord operationRecord, InformixTransactionCache transactionCache, boolean recover) throws IfxStreamException, SQLException {
        Long transId = (long) operationRecord.getTransactionId();

        Map<String, IfmxReadableType> dataBefore = transactionCache.afterUpdate(transId).orElse(Map.of());
        Map<String, IfmxReadableType> dataAfter = operationRecord.getData();

        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(operationRecord.getLabel()));
        handleEvent(tid, partition, offsetContext, transId, UPDATE, dataBefore, dataAfter, clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            TxLogPosition.LSN_NULL,
                            operationRecord.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }
    }

    public void handleBegin(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCBeginTransactionRecord beginTransactionRecord, InformixTransactionCache transactionCache, boolean recover) {
        long _start = System.nanoTime();

        Long transId = (long) beginTransactionRecord.getTransactionId();
        Long beginTs = beginTransactionRecord.getTime();
        Long seqId = beginTransactionRecord.getSequenceId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.beginTxn(transId, beginTs, seqId);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
            Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : beginTransactionRecord.getSequenceId();

            if (transactionCacheBuffer.isEmpty()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                beginTransactionRecord.getSequenceId(),
                                transId,
                                beginTransactionRecord.getSequenceId()));

                offsetContext.getTransactionContext().beginTransaction(String.valueOf(beginTransactionRecord.getTransactionId()));
            }
        }

        long _end = System.nanoTime();

        LOGGER.info("Received BEGIN :: transId={} seqId={} time={} userId={} elapsedTs={}ms",
                beginTransactionRecord.getTransactionId(), beginTransactionRecord.getSequenceId(),
                beginTransactionRecord.getTime(), beginTransactionRecord.getUserId(),
                (_end - _start) / 1000000d);
    }

    public void handleCommit(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCCommitTransactionRecord commitTransactionRecord, InformixTransactionCache transactionCache, boolean recover) throws InterruptedException {
        long _start = System.nanoTime();
        Long transId = (long) commitTransactionRecord.getTransactionId();
        Long endTime = commitTransactionRecord.getTime();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.commitTxn(transId, endTime);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
            Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : commitTransactionRecord.getSequenceId();

            if (transactionCacheBuffer.isPresent()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                commitTransactionRecord.getSequenceId(),
                                transId,
                                TxLogPosition.LSN_NULL));

                for (InformixTransactionCache.TransactionCacheRecord r : transactionCacheBuffer.get().getTransactionCacheRecords()) {
                    dispatcher.dispatchDataChangeEvent(partition, r.getTableId(), r.getInformixChangeRecordEmitter());
                }
                LOGGER.info("Handle Commit {} Events, transElapsedTime={}",
                        transactionCacheBuffer.get().size(), transactionCacheBuffer.get().getElapsed());
            }
            offsetContext.getTransactionContext().endTransaction();
        }

        long _end = System.nanoTime();
        LOGGER.info("Received COMMIT :: transId={} seqId={} time={} elapsedTime={} ms",
                commitTransactionRecord.getTransactionId(), commitTransactionRecord.getSequenceId(),
                commitTransactionRecord.getTime(),
                (_end - _start) / 1000000d);
    }

    public void handleInsert(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCOperationRecord operationRecord, InformixTransactionCache transactionCache, boolean recover) throws IfxStreamException, SQLException {
        long _start = System.nanoTime();
        Long transId = (long) operationRecord.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
        Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : operationRecord.getSequenceId();

        Map<String, IfmxReadableType> dataAfter = operationRecord.getData();
        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(operationRecord.getLabel()));
        handleEvent(tid, partition, offsetContext, transId, INSERT, null, dataAfter, clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            minSeqId,
                            operationRecord.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

        long _end = System.nanoTime();
        LOGGER.info("Received INSERT :: transId={} seqId={} elapsedTime={} ms",
                operationRecord.getTransactionId(), operationRecord.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleRollback(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCRollbackTransactionRecord rollbackTransactionRecord, InformixTransactionCache transactionCache, boolean recover) {
        long _start = System.nanoTime();
        Long transId = (long) rollbackTransactionRecord.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> transactionCacheBuffer = transactionCache.rollbackTxn(transId);
        if (!recover) {
            Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
            Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : rollbackTransactionRecord.getSequenceId();

            if (minTransactionCache.isPresent()) {
                minSeqId = minTransactionCache.get().getBeginSeqId();
            }
            if (transactionCacheBuffer.isPresent()) {
                offsetContext.setChangePosition(
                        TxLogPosition.cloneAndSet(
                                offsetContext.getChangePosition(),
                                minSeqId,
                                rollbackTransactionRecord.getSequenceId(),
                                transId,
                                TxLogPosition.LSN_NULL));

                LOGGER.info("Rollback Txn: {}", rollbackTransactionRecord.getTransactionId());
            }
            offsetContext.getTransactionContext().endTransaction();
        }

        long _end = System.nanoTime();
        LOGGER.info("Received ROLLBACK :: transId={} seqId={} elapsedTime={} ms",
                rollbackTransactionRecord.getTransactionId(), rollbackTransactionRecord.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleTruncate(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCTruncateRecord truncateRecord, InformixTransactionCache transactionCache, boolean recover) throws SQLException {
        long _start = System.nanoTime();
        Long transId = (long) truncateRecord.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
        Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : truncateRecord.getSequenceId();

        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(truncateRecord.getUserId());
        handleEvent(tid, partition, offsetContext, transId, TRUNCATE, null, null, clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            minSeqId,
                            truncateRecord.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

        long _end = System.nanoTime();
        LOGGER.info("Received TRUNCATE :: transId={} seqId={} elapsedTime={} ms",
                truncateRecord.getTransactionId(), truncateRecord.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleDelete(InformixCDCEngine cdcEngine, InformixPartition partition, InformixOffsetContext offsetContext, IfxCDCOperationRecord operationRecord, InformixTransactionCache transactionCache, boolean recover) throws IfxStreamException, SQLException {
        long _start = System.nanoTime();
        Long transId = (long) operationRecord.getTransactionId();

        Optional<InformixTransactionCache.TransactionCacheBuffer> minTransactionCache = transactionCache.getMinTransactionCache();
        Long minSeqId = minTransactionCache.isPresent() ? minTransactionCache.get().getBeginSeqId() : operationRecord.getSequenceId();

        Map<String, IfmxReadableType> dataBefore = operationRecord.getData();
        Map<Integer, TableId> label2TableId = cdcEngine.convertLabel2TableId();
        TableId tid = label2TableId.get(Integer.parseInt(operationRecord.getLabel()));
        handleEvent(tid, partition, offsetContext, transId, DELETE, dataBefore, null, clock);

        if (!recover) {
            offsetContext.setChangePosition(
                    TxLogPosition.cloneAndSet(
                            offsetContext.getChangePosition(),
                            minSeqId,
                            operationRecord.getSequenceId(),
                            transId,
                            TxLogPosition.LSN_NULL));
        }

        long _end = System.nanoTime();
        LOGGER.info("Received DELETE :: transId={} seqId={} elapsedTime={} ms",
                operationRecord.getTransactionId(), operationRecord.getSequenceId(),
                (_end - _start) / 1000000d);
    }

    public void handleEvent(TableId tableId,
                            InformixPartition partition,
                            InformixOffsetContext offsetContext,
                            Long txId,
                            Integer operation,
                            Map<String, IfmxReadableType> dataBefore,
                            Map<String, IfmxReadableType> dataAfter,
                            Clock clock)
            throws SQLException {

        offsetContext.event(tableId, clock.currentTime());

        TableSchema tableSchema = schema.schemaFor(tableId);
        InformixChangeRecordEmitter informixChangeRecordEmitter = new InformixChangeRecordEmitter(partition, offsetContext, operation,
                InformixChangeRecordEmitter.convertIfxData2Array(dataBefore, tableSchema),
                InformixChangeRecordEmitter.convertIfxData2Array(dataAfter, tableSchema), clock);

        offsetContext.getInformixTransactionCache().addEvent2Tx(tableId, informixChangeRecordEmitter, txId);
    }
}
