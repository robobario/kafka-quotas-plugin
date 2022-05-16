/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class EnsureTopicAvailableRunnable implements Runnable {
    private final KafkaClientManager kafkaClientManager;
    private final String topic;

    private final Logger log = getLogger(EnsureTopicAvailableRunnable.class);
    private final int partitionCount;

    public EnsureTopicAvailableRunnable(KafkaClientManager kafkaClientManager, String topic, int configPartitionCount) {
        this.kafkaClientManager = kafkaClientManager;
        this.topic = topic;
        partitionCount = configPartitionCount;
    }

    @Override
    public void run() {
        try {
            ensureTopicIsAvailable(topic, partitionCount).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("problem ensuring topic {} is available on the cluster.", topic, e);
        } catch (ExecutionException e) {
            log.error("problem ensuring topic {} is available on the cluster.", topic, e);
        }
    }

    /*test*/ CompletableFuture<Void> ensureTopicIsAvailable(String topic, int partitionCount) {
        final CompletableFuture<Void> createTopicFuture = new CompletableFuture<>();
        log.debug("ensuring {} exists", topic);

        final NewTopic newTopic = buildNewTopic(topic, partitionCount);
        kafkaClientManager.adminClient()
                .createTopics(List.of(newTopic))
                .all()
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        if (isTopicExitsException(throwable)) {
                            log.debug("{} already exists.", topic);
                        } else {
                            log.warn("Error creating topic: {}.", topic, throwable);
                            createTopicFuture.completeExceptionally(throwable);
                            return;
                        }
                    } else {
                        log.info("Created topic: {}", newTopic);
                    }
                    createTopicFuture.complete(null);
                });
        return createTopicFuture;
    }

    private boolean isTopicExitsException(Throwable throwable) {
        return throwable instanceof TopicExistsException || throwable.getCause() instanceof TopicExistsException;
    }

    private NewTopic buildNewTopic(String topic, int partitionCount) {
        final NewTopic newTopic = new NewTopic(topic, Optional.of(partitionCount), Optional.empty());
        newTopic.configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));
        return newTopic;
    }
}
