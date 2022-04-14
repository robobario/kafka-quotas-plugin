/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MinFreeBytesLimitPolicyTest {

    private MinFreeBytesLimitPolicy minFreeBytesLimitPolicy;

    @BeforeEach
    void setUp() {
        minFreeBytesLimitPolicy = new MinFreeBytesLimitPolicy(10L);
    }

    @Test
    void shouldBreachLimitIfFreeBytesEqualToLimit() {
        //Given

        //When
        final boolean actualLimitBreach = minFreeBytesLimitPolicy.breachesLimit(TestUtils.newVolumeWith(10L));

        //Then
        assertThat(actualLimitBreach).isTrue();
    }

    @Test
    void shouldBreachLimitIfFreeBytesPastLimit() {
        //Given

        //When
        final boolean actualLimitBreach = minFreeBytesLimitPolicy.breachesLimit(TestUtils.newVolumeWith(15L));

        //Then
        assertThat(actualLimitBreach).isTrue();
    }

    @Test
    void shouldNotBreachLimitIfFreeBytesUnderLimit() {
        //Given

        //When
        final boolean actualLimitBreach = minFreeBytesLimitPolicy.breachesLimit(TestUtils.newVolumeWith(5L));

        //Then
        assertThat(actualLimitBreach).isFalse();
    }

    @Test
    void shouldReturnDifferenceBetweenMinFreeBytesAndFreeSpace() {
        //Given

        //When
        final long actualBreachQuantity = minFreeBytesLimitPolicy.getBreachQuantity(TestUtils.newVolumeWith(15L));

        //Then
        assertThat(actualBreachQuantity).isEqualTo(5L);
    }
}
