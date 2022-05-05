/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.concurrent.TimeUnit;

/**
 * Describes a repeatable task which generates metrics for the plugin to act on.
 * It is up to the Task 
 */
public interface DataSourceTask extends Runnable {

    /**
     * @return The amount of time between executions of the task.
     */
    long getPeriod();

    /**
     * @return The time unit to quantify the time between executions.
     */
    TimeUnit getPeriodUnit();
}
