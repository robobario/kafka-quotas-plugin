/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MinFreePercentageLimitPolicyTest {
    private MinFreePercentageLimitPolicy minFreePercentageLimitPolicy;

    @BeforeEach
    void setUp() {
        minFreePercentageLimitPolicy = new MinFreePercentageLimitPolicy(0.1);
    }

    @Test
    void shouldBreachLimitIfFreeBytesEqualToLimit() {
        //Given

        //When
        final boolean actualLimitBreach = minFreePercentageLimitPolicy.breachesLimit(TestUtils.newVolumeWith(18L));

        //Then
        assertThat(actualLimitBreach).isTrue();
    }

    @Test
    void shouldBreachLimitIfFreeBytesPastLimit() {
        //Given

        //When
        final boolean actualLimitBreach = minFreePercentageLimitPolicy.breachesLimit(TestUtils.newVolumeWith(19L));

        //Then
        assertThat(actualLimitBreach).isTrue();
    }

    @Test
    void shouldNotBreachLimitIfFreeBytesUnderLimit() {
        //Given

        //When
        final boolean actualLimitBreach = minFreePercentageLimitPolicy.breachesLimit(TestUtils.newVolumeWith(5L));

        //Then
        assertThat(actualLimitBreach).isFalse();
    }

    @Test
    void shouldReturnDifferenceBetweenMinFreeBytesAndFreeSpace() {
        //Given
        minFreePercentageLimitPolicy = new MinFreePercentageLimitPolicy(0.5);

        //When
        final long actualBreachQuantity = minFreePercentageLimitPolicy.getBreachQuantity(TestUtils.newVolumeWith(15L));

        //Then
        assertThat(actualBreachQuantity).isEqualTo(5L);
    }
}
