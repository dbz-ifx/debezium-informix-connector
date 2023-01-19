/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import com.informix.jdbc.IfmxReadableType;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

public class InformixChangeRecordEmitter extends RelationalChangeRecordEmitter<InformixPartition> {

    public static final int INSERT = 1;
    public static final int UPDATE = 2;
    public static final int DELETE = 3;
    public static final int TRUNCATE = 4;

    private final int operation;
    private final Object[] dataBefore;
    private final Object[] dataAfter;

    public InformixChangeRecordEmitter(InformixPartition partition, OffsetContext offset, Integer operation, Object[] dataBefore, Object[] dataAfter, Clock clock) {
        super(partition, offset, clock);

        this.operation = operation;
        this.dataBefore = dataBefore;
        this.dataAfter = dataAfter;
    }

    @Override
    public Operation getOperation() {
        switch (operation) {
            case INSERT:
                return Operation.CREATE;
            case UPDATE:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            case TRUNCATE:
                return Operation.TRUNCATE;
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + operation);
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case READ:
            case TRUNCATE:
                return null;
            default:
                return dataBefore;
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        switch (getOperation()) {
            case DELETE:
            case TRUNCATE:
                return null;
            default:
                return dataAfter;
        }
    }

    /**
     * Convert columns data from Map[String,IfmxReadableType] to Object[].
     * Debezium can't convert the IfmxReadableType object to kafka direct,so use map[AnyRef](x=>x.toObject) to extract the jave type value
     * from IfmxReadableType and pass to debezium for kafka
     *
     * @param data the data from informix cdc map[String,IfmxReadableType].
     *
     * @author Laoflch Luo, Xiaolin Zhang
     */
    public static Object[] convertIfxData2Array(Map<String, IfmxReadableType> data, TableSchema tableSchema) throws SQLException {
        if (data == null) {
            return new Object[0];
        }

        List<Object> list = new ArrayList<>();
        for (Field field : tableSchema.valueSchema().fields()) {
            IfmxReadableType ifmxReadableType = data.get(field.name());
            Object toObject = ifmxReadableType.toObject();
            list.add(toObject);
        }
        return list.toArray();
    }

    @Override
    protected void emitTruncateRecord(Receiver<InformixPartition> receiver, TableSchema tableSchema) throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }
}
