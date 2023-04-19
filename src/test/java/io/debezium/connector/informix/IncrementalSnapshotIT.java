/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static io.debezium.connector.informix.util.TestHelper.TEST_DATABASE;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration.Builder;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotTest;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.util.Testing;

public class IncrementalSnapshotIT extends AbstractIncrementalSnapshotTest<InformixConnector> {

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();
    private InformixConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute("CREATE TABLE IF NOT EXISTS a (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE IF NOT EXISTS b (pk int not null, aa int, primary key (pk))",
                "CREATE TABLE IF NOT EXISTS debezium_signal (id varchar(64), type varchar(32), data varchar(255))");
        connection.execute("TRUNCATE TABLE a", "TRUNCATE TABLE b", "TRUNCATE TABLE debezium_signal");

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.rollback();
            /*
             * connection.execute(
             * "DROP TABLE IF EXISTS a",
             * "DROP TABLE IF EXISTS b",
             * "DROP TABLE IF EXISTS debezium_signal");
             */
            connection.close();
        }
    }

    @Override
    protected Class<InformixConnector> connectorClass() {
        return InformixConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected String topicName() {
        return "testdb.informix.a";
    }

    @Override
    protected List<String> topicNames() {
        return List.of(topicName(), "testdb.informix.b");
    }

    @Override
    protected String tableDataCollectionId() {
        return TEST_DATABASE + '.' + tableName();
    }

    @Override
    protected List<String> tableDataCollectionIds() {
        return tableNames().stream().map(name -> TEST_DATABASE + '.' + name).collect(Collectors.toList());
    }

    @Override
    protected String tableName() {
        return "informix.a";
    }

    @Override
    protected List<String> tableNames() {
        return List.of(tableName(), "informix.b");
    }

    @Override
    protected String signalTableName() {
        return "debezium_signal";
    }

    @Override
    protected Builder config() {
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(InformixConnectorConfig.SIGNAL_DATA_COLLECTION, "testdb.informix.debezium_signal")
                .with(InformixConnectorConfig.SCHEMA_INCLUDE_LIST, "informix")
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, tableDataCollectionId())
                .with(InformixConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10);
    }

    @Override
    protected Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {
        final String tableIncludeList;
        if (signalTableOnly) {
            tableIncludeList = "testdb.informix.b";
        }
        else {
            tableIncludeList = "testdb.informix.a,testdb.informix.b";
        }
        return TestHelper.defaultConfig()
                .with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.SIGNAL_DATA_COLLECTION, "testdb.informix.debezium_signal")
                .with(InformixConnectorConfig.SCHEMA_INCLUDE_LIST, "informix")
                .with(InformixConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL, storeOnlyCapturedDdl);
    }

    @Override
    protected int defaultIncrementalSnapshotChunkSize() {
        return 10;
    }

    @Override
    @Test
    @Ignore // Cannot perform this operation on a table defined for replication
    public void snapshotPreceededBySchemaChange() {
    }

    @Override
    protected void waitForCdcTransactionPropagation(int expectedTransactions) {
        waitForAvailableRecords(Durations.FIVE_SECONDS);
    }

    @Override
    protected boolean waitForAvailableRecords(long timeout, TimeUnit unit) {
        return waitForAvailableRecords(Duration.of(timeout, unit.toChronoUnit()));
    }

    protected boolean waitForAvailableRecords(Duration timeout) {
        try {
            Duration pollInterval = timeout.compareTo(Durations.FIVE_SECONDS) < 0 ? Durations.ONE_HUNDRED_MILLISECONDS : Durations.ONE_SECOND;
            Awaitility.given().pollInterval(pollInterval).await().atMost(timeout).until(() -> !this.consumedLines.isEmpty());
        }
        catch (ConditionTimeoutException ignored) {
        }

        return !this.consumedLines.isEmpty();
    }
}
