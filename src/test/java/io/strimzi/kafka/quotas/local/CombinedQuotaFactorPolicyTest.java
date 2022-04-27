/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import io.strimzi.kafka.quotas.TestUtils;
import io.strimzi.kafka.quotas.policy.ConsumedBytesLimitPolicy;
import io.strimzi.kafka.quotas.types.Volume;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CombinedQuotaFactorPolicyTest {
    private CombinedQuotaFactorPolicy quotaPolicy;

    @BeforeEach
    void setUp() {
        quotaPolicy = new CombinedQuotaFactorPolicy(new ConsumedBytesLimitPolicy(10L), new ConsumedBytesLimitPolicy(15L));
    }

    @Test
    void shouldReturnZeroQuotaFactorIfHardLimitBreached() {
        //Given
        final Volume diskOne = TestUtils.newVolumeWith(20L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(0.0D, quotaFactor, TestUtils.EPSILON);
    }

    @Test
    void shouldReturnZeroQuotaFactorIfUsageIsEqualToHardLimit() {
        //Given
        final Volume diskOne = TestUtils.newVolumeWith(15L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(0.0D, quotaFactor, TestUtils.EPSILON);
    }

    @Test
    void shouldGenerateProportionalQuotaFactorIfUsageBetweenSoftAndHardLimits() {
        //Given
        final Volume diskOne = TestUtils.newVolumeWith(12L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(0.6D, quotaFactor, TestUtils.EPSILON);
    }

    @Test
    void shouldGenerateQuotaFactorWhenBreachingEqualHardAndSoftFactors() {
        //Given
        quotaPolicy = new CombinedQuotaFactorPolicy(new ConsumedBytesLimitPolicy(10L), new ConsumedBytesLimitPolicy(10L));
        final Volume diskOne = TestUtils.newVolumeWith(12L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(0.0D, quotaFactor, TestUtils.EPSILON);
    }

    @Test
    void shouldGenerateFactorOfOneWhenUnderLimit() {
        //Given
        quotaPolicy = new CombinedQuotaFactorPolicy(new ConsumedBytesLimitPolicy(10L), new ConsumedBytesLimitPolicy(15L));
        final Volume diskOne = TestUtils.newVolumeWith(8L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(1.0D, quotaFactor, TestUtils.EPSILON);
    }

    @Test
    void shouldGenerateFactorOfOneWhenUnderIdenticalLimits() {
        //Given
        quotaPolicy = new CombinedQuotaFactorPolicy(new ConsumedBytesLimitPolicy(10L), new ConsumedBytesLimitPolicy(10L));
        final Volume diskOne = TestUtils.newVolumeWith(8L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(1.0D, quotaFactor, TestUtils.EPSILON);
    }

}
