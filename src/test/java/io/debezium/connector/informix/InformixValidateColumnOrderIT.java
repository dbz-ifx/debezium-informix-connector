/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import static io.debezium.config.CommonConnectorConfig.TOMBSTONES_ON_DELETE;
import static io.debezium.connector.informix.InformixConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.informix.InformixConnectorConfig.SnapshotMode.SCHEMA_ONLY;
import static io.debezium.connector.informix.util.TestHelper.TEST_DATABASE;
import static io.debezium.connector.informix.util.TestHelper.defaultConfig;
import static io.debezium.connector.informix.util.TestHelper.testConnection;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Durations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.util.Strings;

import lombok.SneakyThrows;

public class InformixValidateColumnOrderIT extends InformixAbstractConnectorTest {

    private static final String testTableName = "test_column_order";
    private static final Map<String, String> testTableColumns = new LinkedHashMap<>() {
        {
            put("id", "int");
            put("name", "varchar(50)");
            put("age", "int");
            put("gender", "char(10)");
            put("address", "varchar(50)");
        }
    };
    private InformixConnection connection;

    public static void assertRecordInRightOrder(Struct record, Map<String, String> recordToBeCheck) {
        recordToBeCheck.keySet().forEach(field -> assertThat(record.get(field).toString().trim()).isEqualTo(recordToBeCheck.get(field)));
    }

    @Before
    @SneakyThrows
    public void before() {
        connection = testConnection();

        String columns = testTableColumns.entrySet().stream().map(e -> e.getKey() + ' ' + e.getValue()).collect(Collectors.joining(", "));
        connection.execute(String.format("create table if not exists %s(%s)", testTableName, columns));
        connection.execute(String.format("truncate table %s", testTableName));

        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Print.enable();
    }

    @After
    @SneakyThrows
    public void after() {
        if (connection != null) {
            connection.rollback();
            // connection.execute(String.format("drop table if exists %s", testTableName));
            connection.close();
        }
        stopConnector();
    }

    @Test
    @SneakyThrows
    public void testColumnOrderWhileInsert() {

        final Configuration config = defaultConfig().with(SNAPSHOT_MODE, SCHEMA_ONLY).build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("informix_server", "testdb");
        consumeRecords(0);
        waitForStreamingRunning("informix_server", "testdb");

        // insert a record
        Map<String, String> recordToBeInsert = new LinkedHashMap<>() {
            {
                put("id", "1");
                put("name", "cc");
                put("age", "18");
                put("gender", "male");
                put("address", "ff:ff:ff:ff:ff:ff");
            }
        };
        connection.execute(String.format("insert into %s(%s) values(\"%s\")", testTableName, Strings.join(", ", recordToBeInsert.keySet()),
                Strings.join("\", \"", recordToBeInsert.values())));

        waitForAvailableRecords(Durations.TEN_SECONDS);

        String topicName = String.format("%s.informix.%s", TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> insertOne = sourceRecords.recordsForTopic(topicName);
        assertThat(insertOne).isNotNull().hasSize(1);

        final SourceRecord insertedOneRecord = insertOne.get(0);
        final Struct insertedOneValue = (Struct) insertedOneRecord.value();

        VerifyRecord.isValidInsert(insertedOneRecord);
        assertRecordInRightOrder((Struct) insertedOneValue.get("after"), recordToBeInsert);
    }

    @Test
    @SneakyThrows
    public void testColumnOrderWhileUpdate() {

        // insert a record for testing update
        Map<String, String> recordToBeUpdate = new LinkedHashMap<>() {
            {
                put("id", "2");
                put("name", "cc");
                put("age", "18");
                put("gender", "male");
                put("address", "ff:ff:ff:ff:ff:ff");
            }
        };
        connection.execute(String.format("insert into %s(%s) values(\"%s\")", testTableName, Strings.join(", ", recordToBeUpdate.keySet()),
                Strings.join("\", \"", recordToBeUpdate.values())));

        final Configuration config = defaultConfig().with(SNAPSHOT_MODE, SCHEMA_ONLY).build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("informix_server", "testdb");
        consumeRecords(0);
        waitForStreamingRunning("informix_server", "testdb");

        Map<String, String> recordAfterUpdate = new LinkedHashMap<>(recordToBeUpdate);
        // new value
        recordAfterUpdate.put("address", "00:00:00:00:00:00");

        // update
        connection
                .execute(String.format("update %s set address = \"%s\" where id = \"%s\"", testTableName, recordAfterUpdate.get("address"), recordToBeUpdate.get("id")));

        waitForAvailableRecords(Durations.TEN_SECONDS);

        String topicName = String.format("%s.informix.%s", TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> updateOne = sourceRecords.recordsForTopic(topicName);
        assertThat(updateOne).isNotNull().hasSize(1);

        final SourceRecord updatedOneRecord = updateOne.get(0);
        final Struct updatedOneValue = (Struct) updatedOneRecord.value();

        VerifyRecord.isValidUpdate(updatedOneRecord);

        // assert in order
        assertRecordInRightOrder((Struct) updatedOneValue.get("before"), recordToBeUpdate);
        assertRecordInRightOrder((Struct) updatedOneValue.get("after"), recordAfterUpdate);
    }

    @Test
    @SneakyThrows
    public void testColumnOrderWhileDelete() {

        // insert a record to delete
        Map<String, String> recordToBeDelete = new LinkedHashMap<>() {
            {
                put("id", "3");
                put("name", "cc");
                put("age", "18");
                put("gender", "male");
                put("address", "ff:ff:ff:ff:ff:ff");
            }
        };
        connection.execute(String.format("insert into %s(%s) values(\"%s\")", testTableName, Strings.join(", ", recordToBeDelete.keySet()),
                Strings.join("\", \"", recordToBeDelete.values())));

        final Configuration config = defaultConfig().with(SNAPSHOT_MODE, SCHEMA_ONLY).with(TOMBSTONES_ON_DELETE, false).build();

        start(InformixConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("informix_server", "testdb");
        consumeRecords(0);
        waitForStreamingRunning("informix_server", "testdb");

        connection.execute(String.format("delete from %s where id = \"%s\"", testTableName, recordToBeDelete.get("id")));

        waitForAvailableRecords(Durations.TEN_SECONDS);

        String topicName = String.format("%s.informix.%s", TEST_DATABASE, testTableName);
        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> deletedRecords = sourceRecords.recordsForTopic(topicName);

        assertThat(deletedRecords).isNotNull().hasSize(1);

        final SourceRecord deletedOneRecord = deletedRecords.get(0);
        final Struct deletedOneValue = (Struct) deletedOneRecord.value();

        VerifyRecord.isValidDelete(deletedOneRecord);

        // assert in order
        assertRecordInRightOrder((Struct) deletedOneValue.get("before"), recordToBeDelete);
    }

}
