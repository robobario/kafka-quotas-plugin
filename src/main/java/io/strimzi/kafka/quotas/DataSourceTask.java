/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.concurrent.TimeUnit;

public interface DataSourceTask extends Runnable {
    long getPeriod();

    TimeUnit getPeriodUnit();
}
