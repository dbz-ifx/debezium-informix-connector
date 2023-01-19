package io.debezium.connector.informix;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class InformixPartition extends AbstractPartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public InformixPartition(String serverName, String databaseName) {
        super(databaseName);
        this.serverName = serverName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final InformixPartition other = (InformixPartition) o;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "InformixPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    static class Provider implements Partition.Provider<InformixPartition> {
        private final InformixConnectorConfig connectorConfig;

        Provider(InformixConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<InformixPartition> getPartitions() {
            return Collections.singleton(new InformixPartition(connectorConfig.getLogicalName(), connectorConfig.getDatabaseName()));
        }
    }

}
