/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaQuotaFactorSupplierTest {

    private static final Offset<Double> EPSILON = Offset.offset(0.00001);
    public static final String TEST_TOPIC = "testTopic";
    private MockConsumer<String, UpdateQuotaFactor> kafkaConsumer;
    private KafkaQuotaFactorSupplier kafkaQuotaFactorSupplier;

    @BeforeEach
    void setUp() {
        this.kafkaConsumer = initialiseMockConsumer();
        kafkaQuotaFactorSupplier = new KafkaQuotaFactorSupplier(".*", kafkaConsumer);
    }

    @Test
    void shouldAcceptUpdate() {
        //Given
        final double newFactor = 1.0;
        kafkaQuotaFactorSupplier.start();
        kafkaConsumer.addRecord(new ConsumerRecord<>(TEST_TOPIC, 0, 0, "1", new UpdateQuotaFactor(Instant.now(), newFactor)));

        //When
        kafkaQuotaFactorSupplier.run();

        //Then
        assertThat(kafkaQuotaFactorSupplier.get()).isEqualTo(newFactor, EPSILON);
    }

    //TODO something with `validFrom`
    //TODO tests around multiple updates smallest wins? freshest?

    private MockConsumer<String, UpdateQuotaFactor> initialiseMockConsumer() {
        MockConsumer<String, UpdateQuotaFactor> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, 0);
        mockConsumer.updatePartitions(TEST_TOPIC, List.of(new PartitionInfo(TEST_TOPIC, 0, Node.noNode(), new Node[]{Node.noNode()}, new Node[]{Node.noNode()}))); //TODO this will break using KafkaFactorSupplier.start()
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        return mockConsumer;
    }
}
