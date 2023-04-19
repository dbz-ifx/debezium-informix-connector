/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import static io.debezium.connector.informix.util.TestHelper.defaultConfig;
import static io.debezium.connector.informix.util.TestHelper.testConnection;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import io.debezium.connector.informix.util.TestHelper;
import io.debezium.data.SourceRecordAssert;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.time.Date;
import io.debezium.util.Testing;

import lombok.SneakyThrows;

public class InformixCdcTypesIT extends InformixAbstractConnectorTest {

    private InformixConnection connection;

    @Before
    @SneakyThrows
    public void before() {
        connection = testConnection();

        connection.execute("create table if not exists test_bigint(a bigint)");
        connection.execute("truncate table test_bigint");
        connection.execute("create table if not exists test_bigserial(a bigserial)");
        connection.execute("truncate table test_bigserial");
        connection.execute("create table if not exists test_char(a char)");
        connection.execute("truncate table test_char");
        connection.execute("create table if not exists test_date(a date)");
        connection.execute("truncate table test_date");
        connection.execute("create table if not exists test_decimal(a decimal)");
        connection.execute("truncate table test_decimal");
        connection.execute("create table if not exists test_decimal_20(a decimal(20))");
        connection.execute("truncate table test_decimal_20");
        connection.execute("create table if not exists test_decimal_20_5(a decimal(20, 5))");
        connection.execute("truncate table test_decimal_20_5");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    @SneakyThrows
    public void after() {
        if (connection != null) {
            connection.rollback();
            // connection.execute("drop table if exists test_bigint");
            // connection.execute("drop table if exists test_bigserial");
            // connection.execute("drop table if exists test_char");
            // connection.execute("drop table if exists test_date");
            // connection.execute("drop table if exists test_decimal");
            // connection.execute("drop table if exists test_decimal_20");
            // connection.execute("drop table if exists test_decimal_20_5");
            connection.close();
        }
        stopConnector();
    }

    @Test
    @SneakyThrows
    public void testTypes() {

        final Configuration config = defaultConfig().with(InformixConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE, DecimalHandlingMode.STRING).build();

        start(InformixConnector.class, config);

        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("informix_server", "testdb");
        consumeRecords(0);
        waitForStreamingRunning("informix_server", "testdb");

        /*
         * bigint
         */
        Long testLongValue = new Random().nextLong();
        insertOneAndValidate("test_bigint", Schema.OPTIONAL_INT64_SCHEMA, testLongValue.toString(), testLongValue);

        /*
         * bigserial
         */
        Long testBigSerialValue = new Random().nextLong();
        insertOneAndValidate("test_bigserial", Schema.INT64_SCHEMA, testBigSerialValue.toString(), testBigSerialValue);

        /*
         * char
         */
        // insertOneAndValidate("test_char", Schema.OPTIONAL_STRING_SCHEMA, "'a'", 'a');

        /*
         * date
         *
         * As described from official manual:
         * "The DATE data type stores the calendar date. DATE data types require four bytes. A
         * calendar date is stored internally as an integer value equal to the number of days
         * since December 31, 1899."
         * - https://www.ibm.com/docs/en/informix-servers/12.10?topic=types-date-data-type
         *
         * TODO: But, as we test locally, it seems the base date is "1970-01-01", not the "1899-12-31".
         */
        String[] arrTestDate = new String[]{ "2022-01-01" };
        for (String strTestDate : arrTestDate) {
            Integer d = Math.toIntExact(diffInDays("1970-01-01", strTestDate));
            insertOneAndValidate("test_date", Date.builder().optional().build(), "'" + strTestDate + "'", d);
        }

        /*
         * decimal
         */
        Map<String, String> decimal_data_expect = new LinkedHashMap<>() {
            {
                put("12.1", "12.1");
                put("22.12345678901234567890", "22.12345678901235"); // Rounded number
                put("12345678901234567890.12345", "12345678901234570000");
            }
        };
        for (Map.Entry<String, String> entry : decimal_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }

        /*
         * decimal(20)
         */
        Map<String, String> decimal_20_data_expect = new LinkedHashMap<>() {
            {
                put("88.07", "88.07");
                put("33.12345", "33.12345"); // Rounded number
                put("123456789012345.12345", "123456789012345.12345");
            }
        };
        for (Map.Entry<String, String> entry : decimal_20_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal_20", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }

        /*
         * decimal(20, 5)
         */
        Map<String, String> decimal_20_5_data_expect = new LinkedHashMap<>() {
            {
                put("12.1", "12.10000");
                put("22.12345", "22.12345"); // Rounded number
                put("123456789012345.12345", "123456789012345.12345");
            }
        };
        for (Map.Entry<String, String> entry : decimal_20_5_data_expect.entrySet()) {
            insertOneAndValidate("test_decimal_20_5", Schema.OPTIONAL_STRING_SCHEMA, entry.getKey(), entry.getValue());
        }
    }

    @SneakyThrows
    private void insertOneAndValidate(String tableName, Schema valueSchema, String insertValue, Object expectValue) {
        String topicName = String.format("testdb.informix.%s", tableName);
        connection.execute(String.format("insert into %s values(%s)", tableName, insertValue));

        waitForAvailableRecords(Durations.TEN_SECONDS);

        List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(topicName);
        assertThat(records).isNotNull().hasSize(1);

        Schema aSchema = SchemaBuilder.struct().optional().name(String.format("%s.Value", topicName)).field("a", valueSchema).build();
        Struct aStruct = new Struct(aSchema).put("a", expectValue);

        SourceRecordAssert.assertThat(records.get(0)).valueAfterFieldIsEqualTo(aStruct);
    }

    private long diffInDays(String one, String other) {
        return LocalDate.parse(one).until(LocalDate.parse(other), ChronoUnit.DAYS);
    }

}
