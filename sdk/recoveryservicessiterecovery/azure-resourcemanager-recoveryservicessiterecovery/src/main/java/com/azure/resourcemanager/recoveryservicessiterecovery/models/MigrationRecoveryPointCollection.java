// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.recoveryservicessiterecovery.models;

import com.azure.core.annotation.Fluent;
import com.azure.resourcemanager.recoveryservicessiterecovery.fluent.models.MigrationRecoveryPointInner;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Collection of migration recovery points. */
@Fluent
public final class MigrationRecoveryPointCollection {
    /*
     * The migration recovery point details.
     */
    @JsonProperty(value = "value")
    private List<MigrationRecoveryPointInner> value;

    /*
     * The value of next link.
     */
    @JsonProperty(value = "nextLink")
    private String nextLink;

    /** Creates an instance of MigrationRecoveryPointCollection class. */
    public MigrationRecoveryPointCollection() {
    }

    /**
     * Get the value property: The migration recovery point details.
     *
     * @return the value value.
     */
    public List<MigrationRecoveryPointInner> value() {
        return this.value;
    }

    /**
     * Set the value property: The migration recovery point details.
     *
     * @param value the value value to set.
     * @return the MigrationRecoveryPointCollection object itself.
     */
    public MigrationRecoveryPointCollection withValue(List<MigrationRecoveryPointInner> value) {
        this.value = value;
        return this;
    }

    /**
     * Get the nextLink property: The value of next link.
     *
     * @return the nextLink value.
     */
    public String nextLink() {
        return this.nextLink;
    }

    /**
     * Set the nextLink property: The value of next link.
     *
     * @param nextLink the nextLink value to set.
     * @return the MigrationRecoveryPointCollection object itself.
     */
    public MigrationRecoveryPointCollection withNextLink(String nextLink) {
        this.nextLink = nextLink;
        return this;
    }

    /**
     * Validates the instance.
     *
     * @throws IllegalArgumentException thrown if the instance is not valid.
     */
    public void validate() {
        if (value() != null) {
            value().forEach(e -> e.validate());
        }
    }
}
