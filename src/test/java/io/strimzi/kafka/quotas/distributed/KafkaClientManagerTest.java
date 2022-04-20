/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.io.IOException;
import java.util.Map;

import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.strimzi.kafka.quotas.distributed.KafkaClientFactory.LISTENER_PORT_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaClientManagerTest {

    public static final String TEST_TOPIC = "Test_topic";
    private KafkaClientManager kafkaClientManager;

    @Mock(lenient = true)
    KafkaClientFactory kafkaClientFactory;

    @Mock(lenient = true)
    Producer<String, VolumeUsageMetrics> producer;
    @Mock(lenient = true)
    Consumer<String, VolumeUsageMetrics> consumer;

    @SuppressWarnings("resource")
    @BeforeEach
    void setUp() {
        doReturn(producer).when(kafkaClientFactory).newProducer(anyMap(), eq(VolumeUsageMetrics.class));
        doReturn(consumer).when(kafkaClientFactory).newConsumer(anyMap(), eq(VolumeUsageMetrics.class));

        kafkaClientManager = new KafkaClientManager(kafkaClientConfig -> kafkaClientFactory);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (kafkaClientManager != null) {
            kafkaClientManager.close();
        }
    }

    @Test
    void shouldNotCreateProducerWithOutConfig() {
        //Given

        //When
        final Producer<String, VolumeUsageMetrics> producer = kafkaClientManager.producer(VolumeUsageMetrics.class);

        //Then
        assertThat(producer).isNull();
    }

    @Test
    void shouldNotCreateConsumerWithOutConfig() {
        //Given

        //When
        final Consumer<String, VolumeUsageMetrics> consumer = kafkaClientManager.consumerFor(TEST_TOPIC, VolumeUsageMetrics.class);

        //Then
        assertThat(consumer).isNull();
    }

    @Test
    void shouldReuseProducer() {
        //Given
        kafkaClientManager.configure(Map.of(LISTENER_PORT_PROP, 9091));
        final Producer<String, VolumeUsageMetrics> initialProducer = kafkaClientManager.producer(VolumeUsageMetrics.class);
        assertThat(initialProducer).isNotNull();

        //When
        final Producer<String, VolumeUsageMetrics> subsequentProducer = kafkaClientManager.producer(VolumeUsageMetrics.class);

        //Then
        assertThat(subsequentProducer).isSameAs(initialProducer);
    }

    @Test
    void shouldReuseConsumer() {
        //Given
        kafkaClientManager.configure(Map.of(LISTENER_PORT_PROP, 9091));
        final Consumer<String, VolumeUsageMetrics> initialConsumer = kafkaClientManager.consumerFor(TEST_TOPIC, VolumeUsageMetrics.class);
        assertThat(initialConsumer).isNotNull();

        //When
        final Consumer<String, VolumeUsageMetrics> subsequentConsumer = kafkaClientManager.consumerFor(TEST_TOPIC, VolumeUsageMetrics.class);

        //Then
        assertThat(subsequentConsumer).isSameAs(initialConsumer);
    }

    @SuppressWarnings({"rawtypes", "resource"})
    @Test
    void shouldNotProducerForDifferentMessageTypes() {
        //Given
        kafkaClientManager.configure(Map.of(LISTENER_PORT_PROP, 9091));
        doReturn(mock(Producer.class)).when(kafkaClientFactory).newProducer(anyMap(), eq(UpdateQuotaFactor.class));
        //Use the raw type so the producers are actually testable.
        //Arguably a redundant test due to the type system but...
        final Producer initialProducer = kafkaClientManager.producer(VolumeUsageMetrics.class);
        assertThat(initialProducer).isNotNull();

        //When
        final Producer subsequentProducer = kafkaClientManager.producer(UpdateQuotaFactor.class);

        //Then
        assertThat(subsequentProducer).isNotNull().isNotSameAs(initialProducer);
    }

    @Test
    @SuppressWarnings({"rawtypes", "resource"})
    void shouldNotReuseConsumerForDifferentMessageTypes() {
        //Given
        kafkaClientManager.configure(Map.of(LISTENER_PORT_PROP, 9091));
        doReturn(mock(Consumer.class)).when(kafkaClientFactory).newConsumer(anyMap(), eq(UpdateQuotaFactor.class));
        //Use the raw type so the producers are actually testable.
        //Arguably a redundant test due to the type system but...
        final Consumer initialConsumer = kafkaClientManager.consumerFor(TEST_TOPIC, VolumeUsageMetrics.class);
        assertThat(initialConsumer).isNotNull();

        //When
        final Consumer subsequentConsumer = kafkaClientManager.consumerFor(TEST_TOPIC, UpdateQuotaFactor.class);

        //Then
        assertThat(subsequentConsumer).isNotNull().isNotSameAs(initialConsumer);
    }

    @Test
    void shouldCloseProducer() throws IOException {
        //Given
        kafkaClientManager.configure(Map.of(LISTENER_PORT_PROP, 9091));
        kafkaClientManager.producer(VolumeUsageMetrics.class);

        //When
        kafkaClientManager.close();

        //Then
        verify(producer).close();
    }

    @Test
    void shouldCloseConsumer() throws IOException {
        //Given
        kafkaClientManager.configure(Map.of(LISTENER_PORT_PROP, 9091));
        kafkaClientManager.consumerFor(TEST_TOPIC, VolumeUsageMetrics.class);

        //When
        kafkaClientManager.close();

        //Then
        verify(consumer).close();
    }
}
