// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.datafactory.generated;

import com.azure.core.util.BinaryData;
import com.azure.resourcemanager.datafactory.models.SelfDependencyTumblingWindowTriggerReference;
import org.junit.jupiter.api.Assertions;

public final class SelfDependencyTumblingWindowTriggerReferenceTests {
    @org.junit.jupiter.api.Test
    public void testDeserialize() throws Exception {
        SelfDependencyTumblingWindowTriggerReference model = BinaryData
            .fromString(
                "{\"type\":\"SelfDependencyTumblingWindowTriggerReference\",\"offset\":\"mny\",\"size\":\"mctn\"}")
            .toObject(SelfDependencyTumblingWindowTriggerReference.class);
        Assertions.assertEquals("mny", model.offset());
        Assertions.assertEquals("mctn", model.size());
    }

    @org.junit.jupiter.api.Test
    public void testSerialize() throws Exception {
        SelfDependencyTumblingWindowTriggerReference model
            = new SelfDependencyTumblingWindowTriggerReference().withOffset("mny").withSize("mctn");
        model = BinaryData.fromObject(model).toObject(SelfDependencyTumblingWindowTriggerReference.class);
        Assertions.assertEquals("mny", model.offset());
        Assertions.assertEquals("mctn", model.size());
    }
}
