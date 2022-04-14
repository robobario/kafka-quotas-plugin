/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.TestUtils;
import io.strimzi.kafka.quotas.types.Volume;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumedBytesLimitPolicyTest {

    private ConsumedBytesLimitPolicy limitPolicy;

    @BeforeEach
    void setUp() {
        limitPolicy = new ConsumedBytesLimitPolicy(10L);
    }

    @Test
    void shouldBreachLimitWhenConsumedBytesEqualToLimit() {
        //Given
        final Volume volume = TestUtils.newVolumeWith(10L);

        //When
        final boolean actualBreach = limitPolicy.breachesLimit(volume);

        //Then
        assertThat(actualBreach).isTrue();
    }


    @Test
    void shouldBreachLimitWhenConsumedBytesOverLimit() {
        //Given
        final Volume volume = TestUtils.newVolumeWith(20L);

        //When
        final boolean actualBreach = limitPolicy.breachesLimit(volume);

        //Then
        assertThat(actualBreach).isTrue();
    }

    @Test
    void shouldNotBreachLimitWhenConsumedBytesUnderLimit() {
        //Given
        final Volume volume = TestUtils.newVolumeWith(8L);

        //When
        final boolean actualBreach = limitPolicy.breachesLimit(volume);

        //Then
        assertThat(actualBreach).isFalse();
    }

    @Test
    void shouldCalculateDifferenceBetweenLimitAndConsumedSpace() {
        //Given
        final Volume volume = TestUtils.newVolumeWith(20L);

        //When
        final long actualBreachQuantity = limitPolicy.getBreachQuantity(volume);

        //Then
        assertThat(actualBreachQuantity).isEqualTo(10L);
    }
}
