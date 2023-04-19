/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import static io.debezium.connector.informix.util.TestHelper.SCHEMA_HISTORY_PATH;
import static io.debezium.connector.informix.util.TestHelper.defaultConfig;
import static io.debezium.connector.informix.util.TestHelper.testConnection;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Durations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode;
import io.debezium.doc.FixFor;

import lombok.SneakyThrows;

/**
 * Integration test for the user-facing history topic of the Debezium Informix Server connector.
 * <p>
 * The tests should verify the {@code CREATE} schema events from snapshot and the {@code CREATE} and
 * the {@code ALTER} schema events from streaming
 *
 */
public class SchemaHistoryTopicIT extends InformixAbstractConnectorTest {

    private InformixConnection connection;

    @Before
    @SneakyThrows
    public void before() {
        connection = testConnection();
        /*
         * Since all DDL operations are forbidden during Informix CDC, we have to prepare all tables for testing.
         */
        connection.execute("CREATE TABLE IF NOT EXISTS tableah (id int not null, cola varchar(30), primary key(id))",
                "CREATE TABLE IF NOT EXISTS tablebh (id int not null, colb varchar(30), primary key(id))",
                "CREATE TABLE IF NOT EXISTS tablech (id int not null, colc varchar(30), primary key(id))");
        connection.execute("TRUNCATE TABLE tableah", "TRUNCATE TABLE tablebh", "TRUNCATE TABLE tablech");

        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
        Print.enable();
    }

    @After
    @SneakyThrows
    public void after() {
        if (connection != null) {
            connection.rollback();
            // connection.execute(
            // "DROP TABLE IF EXISTS tablea",
            // "DROP TABLE IF EXISTS tableb",
            // "DROP TABLE IF EXISTS tablec");
            connection.close();
        }
        stopConnector();
    }

    @Test
    @FixFor("DBZ-1904")
    @SneakyThrows
    public void snapshotSchemaChanges() {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START_1 = 10;
        final Configuration config = defaultConfig().with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(InformixConnectorConfig.INCLUDE_SCHEMA_CHANGES, true).build();

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START_1 + i;
            connection.execute("INSERT INTO tableah VALUES(" + id + ", 'a')");
            connection.execute("INSERT INTO tablebh VALUES(" + id + ", 'b')");
        }

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("informix_server", "testdb");
        waitForStreamingRunning("informix_server", "testdb");

        waitForAvailableRecords(Durations.TEN_SECONDS);

        // DDL for 3 tables
        SourceRecords records = consumeRecordsByTopic(3);
        final List<SourceRecord> schemaRecords = records.allRecordsInOrder();
        assertThat(schemaRecords).hasSize(3);
        schemaRecords.forEach(record -> {
            assertThat(record.topic()).isEqualTo("testdb");
            assertThat(((Struct) record.key()).getString("databaseName")).isEqualTo("testdb");
            assertThat(record.sourceOffset().get("snapshot")).isEqualTo(true);
        });
        assertThat(((Struct) schemaRecords.get(0).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(1).value()).getStruct("source").getString("snapshot")).isEqualTo("true");
        assertThat(((Struct) schemaRecords.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("true");

        final List<Struct> tableChanges = ((Struct) schemaRecords.get(0).value()).getArray("tableChanges");
        assertThat(tableChanges).hasSize(1);
        assertThat(tableChanges.get(0).get("type")).isEqualTo("CREATE");

        records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        assertThat(records.recordsForTopic("testdb.informix.tableah")).hasSize(RECORDS_PER_TABLE);
        assertThat(records.recordsForTopic("testdb.informix.tablebh")).hasSize(RECORDS_PER_TABLE);
        records.recordsForTopic("testdb.informix.tablebh").forEach(record -> assertSchemaMatchesStruct((Struct) ((Struct) record.value()).get("after"), SchemaBuilder
                .struct().optional().name("testdb.informix.tablebh.Value").field("id", Schema.INT32_SCHEMA).field("colb", Schema.OPTIONAL_STRING_SCHEMA).build()));
    }

}
