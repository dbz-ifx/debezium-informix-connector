/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.informix;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.awaitility.core.ConditionTimeoutException;

import io.debezium.embedded.AbstractConnectorTest;

public abstract class InformixAbstractConnectorTest extends AbstractConnectorTest {
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
