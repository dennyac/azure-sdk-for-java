// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.datafactory.generated;

import com.azure.core.util.BinaryData;
import com.azure.resourcemanager.datafactory.models.ConcurSource;

public final class ConcurSourceTests {
    @org.junit.jupiter.api.Test
    public void testDeserialize() throws Exception {
        ConcurSource model = BinaryData.fromString(
            "{\"type\":\"ConcurSource\",\"query\":\"datarctbxpuis\",\"queryTimeout\":\"dataamgnpeosusi\",\"additionalColumns\":\"dataco\",\"sourceRetryCount\":\"datajabd\",\"sourceRetryWait\":\"dataalipbuqkdieuop\",\"maxConcurrentConnections\":\"dataaknhmi\",\"disableMetricsCollection\":\"dataf\",\"\":{\"oy\":\"datafmoonnria\"}}")
            .toObject(ConcurSource.class);
    }

    @org.junit.jupiter.api.Test
    public void testSerialize() throws Exception {
        ConcurSource model
            = new ConcurSource().withSourceRetryCount("datajabd").withSourceRetryWait("dataalipbuqkdieuop")
                .withMaxConcurrentConnections("dataaknhmi").withDisableMetricsCollection("dataf")
                .withQueryTimeout("dataamgnpeosusi").withAdditionalColumns("dataco").withQuery("datarctbxpuis");
        model = BinaryData.fromObject(model).toObject(ConcurSource.class);
    }
}
