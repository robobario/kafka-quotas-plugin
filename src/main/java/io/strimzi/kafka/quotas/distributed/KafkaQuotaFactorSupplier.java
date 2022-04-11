/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import io.strimzi.kafka.quotas.QuotaFactorSupplier;
import io.strimzi.kafka.quotas.types.QuotaFactorUpdate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class KafkaQuotaFactorSupplier implements QuotaFactorSupplier, AutoCloseable, Runnable {
    private final Pattern subscriptionPattern;

    private final Consumer<String, QuotaFactorUpdate> kafkaConsumer;

    private final AtomicLong currentFactor;

    public KafkaQuotaFactorSupplier(String subscriptionPattern, Consumer<String, QuotaFactorUpdate> kafkaConsumer) {
        this.subscriptionPattern = Pattern.compile(subscriptionPattern);
        this.kafkaConsumer = kafkaConsumer;
        currentFactor = new AtomicLong(Double.doubleToLongBits(0.0));
    }

    //TODO should this be on the Interface? Will all implementations use polling? No. So how do we start it?
    public void start() {
        kafkaConsumer.subscribe(subscriptionPattern);
    }

    @Override
    public Double get() {
        return Double.longBitsToDouble(currentFactor.get());
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

    @Override
    public void run() {
        //TODO inject duration
        final ConsumerRecords<String, QuotaFactorUpdate> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10));
        consumerRecords.forEach(cr -> {
            final QuotaFactorUpdate updateMessage = cr.value();
            currentFactor.getAndSet(Double.doubleToLongBits(updateMessage.getFactor()));
        });

    }
}
