// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.redisenterprise.generated;

import com.azure.core.util.BinaryData;
import com.azure.resourcemanager.redisenterprise.fluent.models.DatabaseProperties;
import com.azure.resourcemanager.redisenterprise.models.AofFrequency;
import com.azure.resourcemanager.redisenterprise.models.ClusteringPolicy;
import com.azure.resourcemanager.redisenterprise.models.DatabasePropertiesGeoReplication;
import com.azure.resourcemanager.redisenterprise.models.EvictionPolicy;
import com.azure.resourcemanager.redisenterprise.models.LinkedDatabase;
import com.azure.resourcemanager.redisenterprise.models.Module;
import com.azure.resourcemanager.redisenterprise.models.Persistence;
import com.azure.resourcemanager.redisenterprise.models.Protocol;
import com.azure.resourcemanager.redisenterprise.models.RdbFrequency;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;

public final class DatabasePropertiesTests {
    @org.junit.jupiter.api.Test
    public void testDeserialize() throws Exception {
        DatabaseProperties model =
            BinaryData
                .fromString(
                    "{\"clientProtocol\":\"Encrypted\",\"port\":61111335,\"provisioningState\":\"Failed\",\"resourceState\":\"Running\",\"clusteringPolicy\":\"OSSCluster\",\"evictionPolicy\":\"AllKeysRandom\",\"persistence\":{\"aofEnabled\":true,\"rdbEnabled\":true,\"aofFrequency\":\"always\",\"rdbFrequency\":\"6h\"},\"modules\":[{\"name\":\"xaqwoochcbonqv\",\"args\":\"vlrxnjeaseiph\",\"version\":\"f\"}],\"geoReplication\":{\"groupNickname\":\"eyy\",\"linkedDatabases\":[{\"id\":\"bdlwtgrhpdjpj\",\"state\":\"Unlinking\"}]}}")
                .toObject(DatabaseProperties.class);
        Assertions.assertEquals(Protocol.ENCRYPTED, model.clientProtocol());
        Assertions.assertEquals(61111335, model.port());
        Assertions.assertEquals(ClusteringPolicy.OSSCLUSTER, model.clusteringPolicy());
        Assertions.assertEquals(EvictionPolicy.ALL_KEYS_RANDOM, model.evictionPolicy());
        Assertions.assertEquals(true, model.persistence().aofEnabled());
        Assertions.assertEquals(true, model.persistence().rdbEnabled());
        Assertions.assertEquals(AofFrequency.ALWAYS, model.persistence().aofFrequency());
        Assertions.assertEquals(RdbFrequency.SIXH, model.persistence().rdbFrequency());
        Assertions.assertEquals("xaqwoochcbonqv", model.modules().get(0).name());
        Assertions.assertEquals("vlrxnjeaseiph", model.modules().get(0).args());
        Assertions.assertEquals("eyy", model.geoReplication().groupNickname());
        Assertions.assertEquals("bdlwtgrhpdjpj", model.geoReplication().linkedDatabases().get(0).id());
    }

    @org.junit.jupiter.api.Test
    public void testSerialize() throws Exception {
        DatabaseProperties model =
            new DatabaseProperties()
                .withClientProtocol(Protocol.ENCRYPTED)
                .withPort(61111335)
                .withClusteringPolicy(ClusteringPolicy.OSSCLUSTER)
                .withEvictionPolicy(EvictionPolicy.ALL_KEYS_RANDOM)
                .withPersistence(
                    new Persistence()
                        .withAofEnabled(true)
                        .withRdbEnabled(true)
                        .withAofFrequency(AofFrequency.ALWAYS)
                        .withRdbFrequency(RdbFrequency.SIXH))
                .withModules(Arrays.asList(new Module().withName("xaqwoochcbonqv").withArgs("vlrxnjeaseiph")))
                .withGeoReplication(
                    new DatabasePropertiesGeoReplication()
                        .withGroupNickname("eyy")
                        .withLinkedDatabases(Arrays.asList(new LinkedDatabase().withId("bdlwtgrhpdjpj"))));
        model = BinaryData.fromObject(model).toObject(DatabaseProperties.class);
        Assertions.assertEquals(Protocol.ENCRYPTED, model.clientProtocol());
        Assertions.assertEquals(61111335, model.port());
        Assertions.assertEquals(ClusteringPolicy.OSSCLUSTER, model.clusteringPolicy());
        Assertions.assertEquals(EvictionPolicy.ALL_KEYS_RANDOM, model.evictionPolicy());
        Assertions.assertEquals(true, model.persistence().aofEnabled());
        Assertions.assertEquals(true, model.persistence().rdbEnabled());
        Assertions.assertEquals(AofFrequency.ALWAYS, model.persistence().aofFrequency());
        Assertions.assertEquals(RdbFrequency.SIXH, model.persistence().rdbFrequency());
        Assertions.assertEquals("xaqwoochcbonqv", model.modules().get(0).name());
        Assertions.assertEquals("vlrxnjeaseiph", model.modules().get(0).args());
        Assertions.assertEquals("eyy", model.geoReplication().groupNickname());
        Assertions.assertEquals("bdlwtgrhpdjpj", model.geoReplication().linkedDatabases().get(0).id());
    }
}
