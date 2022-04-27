/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;

/**
 *  Describes a repeatable task which consumes the published metrics for the cluster and determines the currently applicable quota Factor
 */
public interface QuotaFactorPolicyTask extends Runnable {

    /**
     * @return The amount of time between executions of the task.
     */
    long getPeriod();

    /**
     * @return The time unit to quantify the time between executions.
     */
    TimeUnit getPeriodUnit();

    /**
     * Register a listener to be notified when the {@code quotaFactor} changes
     * @param quotaFactorConsumer the listener to be notified
     */
    void addListener(Consumer<UpdateQuotaFactor> quotaFactorConsumer);
}
