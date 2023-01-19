/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.informix;

import com.informix.jdbc.IfxDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

public class InformixConnection extends JdbcConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixConnection.class);

    private static final String QUOTED_CHARACTER = "\"";

    private static final String URL_PATTERN = "jdbc:informix-sqli://${" +
            JdbcConfiguration.HOSTNAME + "}:${" +
            JdbcConfiguration.PORT + "}/${" +
            JdbcConfiguration.DATABASE +
            "}:user=${" + JdbcConfiguration.USER +
            "};password=${" + JdbcConfiguration.PASSWORD + "}";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            IfxDriver.class.getName(),
            InformixConnection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(InformixConnectorConfig.PORT.defaultValueAsString()));

    private final String realDatabaseName;
    private final InformixCDCEngine cdcEngine;

    public InformixConnection(JdbcConfiguration config) {
        super(config, FACTORY, QUOTED_CHARACTER, QUOTED_CHARACTER);

        realDatabaseName = config.getString(JdbcConfiguration.DATABASE);
        cdcEngine = InformixCDCEngine.build(config);
    }

    public InformixCDCEngine getCdcEngine() {
        return cdcEngine;
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }
}
