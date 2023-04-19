/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import static io.debezium.connector.informix.InformixConnectorConfig.CDC_BUFFERSIZE;
import static io.debezium.connector.informix.InformixConnectorConfig.CDC_TIMEOUT;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfxDriver;
import com.informix.jdbcx.IfxDataSource;
import com.informix.stream.cdc.IfxCDCEngine;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

public class InformixConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixConnection.class);

    private static final String GET_DATABASE_NAME = "select dbinfo('dbname') from systables where tabid = 1";
    private static final String CDC_DATABASE = "syscdcv1";
    private static final String GET_MAX_LSN = "select max(seqnum) from syscdcv1:syscdctabs";
    private static final String LOCK_TABLE = "LOCK TABLE %s.%s IN %s MODE"; // DB2
    // TODO: Unless DELIMIDENT is set, column names cannot be quoted
    private static final String QUOTED_CHARACTER = "";
    private static final String URL_PATTERN = "jdbc:informix-sqli://${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${"
            + JdbcConfiguration.DATABASE + "}:user=${" + JdbcConfiguration.USER + "};password=${" + JdbcConfiguration.PASSWORD + "}";

    private static final String CDC_URL_PATTERN = "jdbc:informix-sqli://${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/" + CDC_DATABASE
            + ":user=${" + JdbcConfiguration.USER + "};password=${" + JdbcConfiguration.PASSWORD + "}";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN, IfxDriver.class.getName(), InformixConnection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(InformixConnectorConfig.PORT.defaultValueAsString()));

    /**
     * actual name of the database, which could differ in casing from the database name given in the connector config.
     */
    private final String realDatabaseName;

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public InformixConnection(JdbcConfiguration config) {
        super(config, FACTORY, QUOTED_CHARACTER, QUOTED_CHARACTER);
        realDatabaseName = retrieveRealDatabaseName().trim();
    }

    /**
     * @return the current largest log sequence number
     */
    public Lsn getMaxLsn() throws SQLException {
        /*
         * return queryAndMap(GET_MAX_LSN, singleResultMapper(rs -> {
         * final Lsn lsn = Lsn.valueOf(rs.getLong(1));
         * LOGGER.trace("Current maximum lsn is {}", lsn);
         * return lsn;
         * }, "Maximum LSN query must return exactly one value"));
         */
        return Lsn.valueOf(0x00L);
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(GET_DATABASE_NAME, singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }

    @Override
    public Optional<Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap("SELECT CURRENT YEAR TO FRACTION(5)", rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        // TODO: Unless DELIMIDENT is set, table names cannot be quoted
        StringBuilder builder = new StringBuilder();

        String catalogName = tableId.catalog();
        if (!Strings.isNullOrBlank(catalogName)) {
            builder.append(catalogName).append(':');
        }

        String schemaName = tableId.schema();
        if (schemaName != null && !schemaName.isEmpty()) {
            builder.append(schemaName).append('.');
        }

        return builder.append(tableId.table()).toString();
    }

    @Override
    public String quotedColumnIdString(String columnName) {
        // TODO: Unless DELIMIDENT is set, column names cannot be quoted
        return columnName;
    }

    private IfxCDCEngine getCDCEngine(InformixDatabaseSchema schema, Lsn startLsn) throws InterruptedException {
        IfxCDCEngine engine;
        try {
            IfxDataSource dataSource = new IfxDataSource(connectionString(CDC_URL_PATTERN));
            IfxCDCEngine.Builder builder = IfxCDCEngine.builder(dataSource).buffer(config().getInteger(CDC_BUFFERSIZE)).timeout(config().getInteger(CDC_TIMEOUT));

            schema.tableIds().forEach((TableId tid) -> {
                String[] colNames = schema.tableFor(tid).retrieveColumnNames().toArray(String[]::new);
                builder.watchTable(tid.identifier(), colNames);
            });

            if (startLsn.isAvailable()) {
                builder.sequenceId(startLsn.longValue());
            }
            if (LOGGER.isInfoEnabled()) {
                long seqId = builder.getSequenceId();
                LOGGER.info("Set CDCEngine's LSN to '{}' aka {}", seqId, Lsn.valueOf(seqId).toLongString());
            }

            engine = builder.build();
        }
        catch (SQLException e) {
            LOGGER.error("Caught SQLException", e);
            throw new InterruptedException("Failed while while initialize CDC Engine");
        }
        return engine;
    }

    public InformixCDCTransactionEngine getTransactionEngine(ChangeEventSource.ChangeEventSourceContext context, InformixDatabaseSchema schema, Lsn startLsn)
            throws InterruptedException {
        return new InformixCDCTransactionEngine(context, getCDCEngine(schema, startLsn));
    }
}
