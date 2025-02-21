// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.recoveryservicessiterecovery.generated;

import com.azure.core.util.BinaryData;
import com.azure.resourcemanager.recoveryservicessiterecovery.models.ReverseReplicationInputProperties;
import com.azure.resourcemanager.recoveryservicessiterecovery.models.ReverseReplicationProviderSpecificInput;
import org.junit.jupiter.api.Assertions;

public final class ReverseReplicationInputPropertiesTests {
    @org.junit.jupiter.api.Test
    public void testDeserialize() throws Exception {
        ReverseReplicationInputProperties model =
            BinaryData
                .fromString(
                    "{\"failoverDirection\":\"otmrfhir\",\"providerSpecificDetails\":{\"instanceType\":\"ReverseReplicationProviderSpecificInput\"}}")
                .toObject(ReverseReplicationInputProperties.class);
        Assertions.assertEquals("otmrfhir", model.failoverDirection());
    }

    @org.junit.jupiter.api.Test
    public void testSerialize() throws Exception {
        ReverseReplicationInputProperties model =
            new ReverseReplicationInputProperties()
                .withFailoverDirection("otmrfhir")
                .withProviderSpecificDetails(new ReverseReplicationProviderSpecificInput());
        model = BinaryData.fromObject(model).toObject(ReverseReplicationInputProperties.class);
        Assertions.assertEquals("otmrfhir", model.failoverDirection());
    }
}
