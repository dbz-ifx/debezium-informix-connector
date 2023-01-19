/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import io.debezium.config.*;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.function.Predicates;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.ColumnId;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;

/**
 * The list of configuration options for Informix connector
 *
 * @author laoflch Luo
 */
public class InformixConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    public static final int DEFAULT_QUERY_FETCH_SIZE = 10_000;

    protected static final int DEFAULT_PORT = 9088;

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial", true),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        SCHEMA_ONLY("schema_only", false);


        private final String value;
        private final boolean includeData;


        private SnapshotMode(String value, boolean includeData) {
            this.value = value;
            this.includeData = includeData;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Whether this snapshotting mode should include the actual data or just the
         * schema of captured tables.
         */
        public boolean includeData() {
            return includeData;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    /**
     * The set of predefined snapshot isolation mode options.
     */
    public enum SnapshotIsolationMode implements EnumeratedValue {

        /**
         * This mode will block all reads and writes for the entire duration of the snapshot.
         *
         * The connector will execute {@code SELECT * FROM .. WITH (TABLOCKX)}
         */
         EXCLUSIVE("exclusive"),

        /**
         * This mode uses REPEATABLE READ isolation level. This mode will avoid taking any table
         * locks during the snapshot process, except schema snapshot phase where exclusive table
         * locks are acquired for a short period.  Since phantom reads can occur, it does not fully
         * guarantee consistency.
         */
        REPEATABLE_READ("repeatable_read"),

        /**
         * This mode uses READ COMMITTED isolation level. This mode does not take any table locks during
         * the snapshot process. In addition, it does not take any long-lasting row-level locks, like
         * in repeatable read isolation level. Snapshot consistency is not guaranteed.
         */
        READ_COMMITTED("read_committed"),

        /**
         * This mode uses READ UNCOMMITTED isolation level. This mode takes neither table locks nor row-level locks
         * during the snapshot process.  This way other transactions are not affected by initial snapshot process.
         * However, snapshot consistency is not guaranteed.
         */
        READ_UNCOMMITTED("read_uncommitted");

        private final String value;

        SnapshotIsolationMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotIsolationMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotIsolationMode option : SnapshotIsolationMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotIsolationMode parse(String value, String defaultValue) {
            SnapshotIsolationMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT.withDefault(DEFAULT_PORT);

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_ISOLATION_MODE = Field.create("snapshot.isolation.mode")
            .withDisplayName("Snapshot isolation mode")
            .withEnum(SnapshotIsolationMode.class, SnapshotIsolationMode.REPEATABLE_READ)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls which transaction isolation level is used and how long the connector locks the monitored tables. "
                    + "The default is '" + SnapshotIsolationMode.REPEATABLE_READ.getValue()
                    + "', which means that repeatable read isolation level is used. In addition, exclusive locks are taken only during schema snapshot. "
                    // + "Using a value of '" + SnapshotIsolationMode.EXCLUSIVE.getValue()
                    + "' ensures that the connector holds the exclusive lock (and thus prevents any reads and updates) for all monitored tables during the entire snapshot duration. "
                    + "In '" + SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "' mode no table locks or any *long-lasting* row-level locks are acquired, but connector does not guarantee snapshot consistency."
                    + "In '" + SnapshotIsolationMode.READ_UNCOMMITTED.getValue()
                    + "' mode neither table nor row-level locks are acquired, but connector does not guarantee snapshot consistency.");

    public static final Field QUERY_FETCH_SIZE = CommonConnectorConfig.QUERY_FETCH_SIZE
            .withDescription(
                    "The maximum number of records that should be loaded into memory while streaming. A value of '0' uses the default JDBC fetch size. The default value is '10000'.")
            .withDefault(DEFAULT_QUERY_FETCH_SIZE);

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Informix Server")
            .excluding(CommonConnectorConfig.QUERY_FETCH_SIZE)
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME)
            .connector(
                    SNAPSHOT_MODE,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    QUERY_FETCH_SIZE)
            .create();

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final SnapshotIsolationMode snapshotIsolationMode;
    private final ColumnNameFilter columnFilter;

    public InformixConnectorConfig(Configuration config) {
        super(InformixConnector.class,
                config,
                new SystemTablesPredicate(),
                t -> String.join(".",t.schema(), t.table()),
                false,
                ColumnFilterMode.SCHEMA,
                false);

        this.databaseName = config.getString(DATABASE_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.snapshotIsolationMode = SnapshotIsolationMode.parse(config.getString(SNAPSHOT_ISOLATION_MODE), SNAPSHOT_ISOLATION_MODE.defaultValueAsString());
        this.columnFilter = getColumnNameFilter(config.getString(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST));
    }

    private static ColumnNameFilter getColumnNameFilter(String excludedColumnPatterns) {
        return new ColumnNameFilter() {

            final Predicate<ColumnId> delegate = Predicates.excludes(excludedColumnPatterns, ColumnId::toString);

            @Override
            public boolean matches(String catalogName, String schemaName, String tableName, String columnName) {
                // ignore database name as it's not relevant here
                return delegate.test(new ColumnId(new TableId(null, schemaName, tableName), columnName));
            }
        };
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public SnapshotIsolationMode getSnapshotIsolationMode() {
        return this.snapshotIsolationMode;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public ColumnNameFilter getColumnFilter() {
        return columnFilter;
    }

    @Override
    public SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return new InformixSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return !(t.table().toLowerCase().startsWith("sys"));
        }
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return Lsn.valueOf(recorded.getString(SourceInfo.CHANGE_LSN_KEY))
                        .compareTo(Lsn.valueOf(desired.getString(SourceInfo.CHANGE_LSN_KEY))) < 1;
            }
        };
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    /**
     * Returns any SELECT overrides, if present.
     */
    @Override
    public Map<TableId, String> getSnapshotSelectOverridesByTable() {
        List<String> tableValues = getConfig().getTrimmedStrings(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, ",");

        if (tableValues == null) {
            return Collections.emptyMap();
        }

        Map<TableId, String> snapshotSelectOverridesByTable = new HashMap<>();

        for (String table : tableValues) {
            snapshotSelectOverridesByTable.put(
                    TableId.parse(table, false),
                    getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + table));
        }

        return Collections.unmodifiableMap(snapshotSelectOverridesByTable);
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }
}