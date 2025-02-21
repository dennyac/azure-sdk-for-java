// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.datafactory.generated;

import com.azure.core.util.BinaryData;
import com.azure.resourcemanager.datafactory.fluent.models.FilterActivityTypeProperties;
import com.azure.resourcemanager.datafactory.models.Expression;
import org.junit.jupiter.api.Assertions;

public final class FilterActivityTypePropertiesTests {
    @org.junit.jupiter.api.Test
    public void testDeserialize() throws Exception {
        FilterActivityTypeProperties model
            = BinaryData.fromString("{\"items\":{\"value\":\"frqagpjoci\"},\"condition\":{\"value\":\"nnd\"}}")
                .toObject(FilterActivityTypeProperties.class);
        Assertions.assertEquals("frqagpjoci", model.items().value());
        Assertions.assertEquals("nnd", model.condition().value());
    }

    @org.junit.jupiter.api.Test
    public void testSerialize() throws Exception {
        FilterActivityTypeProperties model = new FilterActivityTypeProperties()
            .withItems(new Expression().withValue("frqagpjoci")).withCondition(new Expression().withValue("nnd"));
        model = BinaryData.fromObject(model).toObject(FilterActivityTypeProperties.class);
        Assertions.assertEquals("frqagpjoci", model.items().value());
        Assertions.assertEquals("nnd", model.condition().value());
    }
}
