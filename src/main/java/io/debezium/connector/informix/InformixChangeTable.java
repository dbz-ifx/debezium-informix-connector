/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import io.debezium.relational.ChangeTable;
import io.debezium.relational.TableId;

public class InformixChangeTable extends ChangeTable {

    private static final String CDC_SCHEMA = "syscdcsv1";

    /**
     * A LSN from which the data in the change table are relevant
     */
    private final Lsn startLsn;

    /**
     * A LSN to which the data in the change table are relevant
     */
    private Lsn stopLsn;

    /**
     * The table in the CDC schema that captures changes, suitably quoted for Informix
     */
    private final String ifxCaptureInstance;

    public InformixChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        super(captureInstance, sourceTableId, resolveChangeTableId(sourceTableId, captureInstance), changeTableObjectId);
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.ifxCaptureInstance = captureInstance;
    }

    public InformixChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn);
    }

    @Override
    public String getCaptureInstance() {
        return ifxCaptureInstance;
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    public Lsn getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
    }

    @Override
    public String toString() {
        return "Capture instance \"" + getCaptureInstance() + "\" [sourceTableId=" + getSourceTableId()
                + ", changeTableId=" + getChangeTableId() + ", startLsn=" + startLsn + ", changeTableObjectId="
                + getChangeTableObjectId() + ", stopLsn=" + stopLsn + "]";
    }

    private static TableId resolveChangeTableId(TableId sourceTableId, String captureInstance) {
        return (sourceTableId != null) ? new TableId(sourceTableId.catalog(), InformixChangeTable.CDC_SCHEMA, captureInstance) : null;
    }
}
