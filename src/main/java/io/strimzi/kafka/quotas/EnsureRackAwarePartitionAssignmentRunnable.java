/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import scala.Int;
import scala.Option;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

import static org.slf4j.LoggerFactory.getLogger;

public class EnsureRackAwarePartitionAssignmentRunnable implements Runnable {
    private static final Logger log = getLogger(EnsureRackAwarePartitionAssignmentRunnable.class);
    private static final Map<Integer, List<Integer>> EMPTY_PARTITION_MAPPING = Collections.emptyMap();
    private final KafkaClientManager kafkaClientManager;
    private final String myBrokerId;
    private final String topic;
    private final int desiredReplicaCount;
    private final Supplier<Collection<String>> activeBrokerSupplier;
    private final Supplier<Collection<Node>> activeNodesSupplier;

    public EnsureRackAwarePartitionAssignmentRunnable(KafkaClientManager kafkaClientManager, Supplier<Collection<String>> activeBrokerSupplier, String topic, int desiredReplicaCount, String myBrokerId, Supplier<Collection<Node>> activeNodesSupplier) {
        this.kafkaClientManager = kafkaClientManager;
        this.myBrokerId = myBrokerId;
        this.topic = topic;
        this.desiredReplicaCount = desiredReplicaCount;
        this.activeBrokerSupplier = activeBrokerSupplier;
        this.activeNodesSupplier = activeNodesSupplier;
    }

    @Override
    public void run() {
        try {
            ensureRackAwarePartitionAssignment(topic, desiredReplicaCount).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("problem ensuring topic {} is safely replicated across the cluster.", topic, e);
        } catch (ExecutionException e) {
            log.error("problem ensuring topic {} is safely replicated across the cluster.", topic, e.getCause());
        }
    }

    /*test*/ CompletableFuture<Void> ensureRackAwarePartitionAssignment(String topic, Integer desiredReplicaCount) {
        final Map<String, String> availableRacks = activeNodesSupplier.get().stream().collect(Collectors.toMap(Node::idString, Node::rack));

        if (isResponsibleBroker() && areThereEnoughRacks(desiredReplicaCount, availableRacks)) {
            //Don't generate a new partition layout unless there are enough racks available to create a resilient layout.
            log.info("ensuring {} is resiliently distributed across available racks: {}", topic, availableRacks.values().stream().distinct().collect(Collectors.joining(", ", "[", "]")));
            return kafkaClientManager.adminClient()
                    .describeTopics(Set.of(topic))
                    .allTopicNames()
                    .toCompletionStage()
                    .thenApply(this::getRacksFromTopicDescription)
                    .thenApply(currentRacks -> generatePartitionToReplicaAssignments(topic, desiredReplicaCount, availableRacks, currentRacks))
                    .thenApply(integerListMap -> convertToPartitionLayout(topic, integerListMap))
                    .thenCompose(partitionLayout -> applyPartitionLayout(topic, partitionLayout))
                    .toCompletableFuture();
        }
        return CompletableFuture.completedFuture(null);
    }

    private static boolean areThereEnoughRacks(Integer desiredReplicaCount, Map<String, String> availableRacks) {
        return desiredReplicaCount == countRacks(availableRacks);
    }

    private boolean isResponsibleBroker() {
        final Optional<String> lowestActiveBroker = activeBrokerSupplier.get().stream().sorted().findFirst();
        return lowestActiveBroker.isPresent() && lowestActiveBroker.get().equalsIgnoreCase(myBrokerId);
    }

    private CompletionStage<Void> applyPartitionLayout(String topic, Map<TopicPartition, Optional<NewPartitionReassignment>> partitionLayout) {
        if (!partitionLayout.isEmpty()) {
            log.info("updating partition layout for: {}", topic);
            return kafkaClientManager.adminClient().alterPartitionReassignments(partitionLayout).all().toCompletionStage();
        } else {
            log.info("The new partition map for {} is empty. No changes will be made.", topic);
            return CompletableFuture.completedFuture(null);
        }
    }

    private int getRacksFromTopicDescription(Map<String, TopicDescription> topicDescriptionsByName) {
        final TopicDescription topicDescription = topicDescriptionsByName.get(topic);
        if (topicDescription == null) {
            log.warn("No description available for topic: {}", topic);
            return -1;
        }
        //We assume a single partition for the metrics topic thus we can flatten to a single map of brokers to racks.
        return topicDescription.partitions().stream()
                .map(TopicPartitionInfo::replicas)
                .flatMap(Collection::stream)
                .filter(Node::hasRack)
                .map(Node::rack)
                .collect(Collectors.toSet())
                .size();
    }

    private static Map<Integer, List<Integer>> generatePartitionToReplicaAssignments(String topic, Integer desiredReplicaCount, Map<String, String> availableRacks, int currentRacks) {
        if (currentRacks != desiredReplicaCount) {
            final Map<Integer, List<Integer>> newPartitionMappings = generateNewPartitionMappings(desiredReplicaCount, availableRacks);
            log.info("Proposing new replica layout for {}. Proposed layout is: {}", topic, newPartitionMappings);
            return newPartitionMappings;
        } else {
            log.info("Replica layout for {} is resilient", topic);
            return EMPTY_PARTITION_MAPPING;
        }
    }

    private static long countRacks(Map<String, String> racksByBroker) {
        return racksByBroker.values().stream().distinct().count();
    }

    private static Map<TopicPartition, Optional<NewPartitionReassignment>> convertToPartitionLayout(String topic, Map<Integer, List<Integer>> integerListMap) {
        return integerListMap.entrySet()
                .stream()
                .map(partitionIdToBrokers -> Map.entry(
                        new TopicPartition(topic, partitionIdToBrokers.getKey()),
                        Optional.of(new NewPartitionReassignment(partitionIdToBrokers.getValue()))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<Integer, List<Integer>> generateNewPartitionMappings(Integer desiredReplicaCount, Map<String, String> racksByBroker) {
        final List<BrokerMetadata> brokerMetadata = racksByBroker.entrySet()
                .stream()
                .map(brokerAndRack -> new BrokerMetadata(Integer.parseInt(brokerAndRack.getKey()), Option.apply(brokerAndRack.getValue())))
                .collect(Collectors.toUnmodifiableList());
        return CollectionConverters.asJava(AdminUtils.assignReplicasToBrokers(CollectionConverters.asScala(brokerMetadata), 1, desiredReplicaCount, 0, 0))
                .entrySet()
                .stream()
                .map(EnsureRackAwarePartitionAssignmentRunnable::convertToJava)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static int toInt(Object candidate) {
        if (candidate instanceof Int) {
            return ((Int) candidate).toInt();
        } else if (candidate instanceof Integer) {
            return (Integer) candidate;
        } else {
            throw new IllegalArgumentException("candidate:" + candidate + " is not an Integer type");
        }
    }

    private static Map.Entry<Integer, List<Integer>> convertToJava(Map.Entry<Object, Seq<Object>> entry) {
        final Integer partitionId = toInt(entry.getKey());
        final Collection<Integer> brokerIds = new ArrayList<>();
        entry.getValue().foreach(v1 -> brokerIds.add(toInt(v1)));

        return Map.entry(partitionId, List.copyOf(brokerIds));
    }
}
