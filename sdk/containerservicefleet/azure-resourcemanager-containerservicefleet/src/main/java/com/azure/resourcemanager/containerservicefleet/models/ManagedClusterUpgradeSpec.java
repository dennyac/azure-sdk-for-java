// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.containerservicefleet.models;

import com.azure.core.annotation.Fluent;
import com.azure.core.util.logging.ClientLogger;
import com.fasterxml.jackson.annotation.JsonProperty;

/** The upgrade to apply to a ManagedCluster. */
@Fluent
public final class ManagedClusterUpgradeSpec {
    /*
     * The upgrade type.
     * Full requires the KubernetesVersion property to be set.
     * NodeImageOnly requires the KubernetesVersion property not to be set.
     */
    @JsonProperty(value = "type", required = true)
    private ManagedClusterUpgradeType type;

    /*
     * The Kubernetes version to upgrade the member clusters to.
     */
    @JsonProperty(value = "kubernetesVersion")
    private String kubernetesVersion;

    /** Creates an instance of ManagedClusterUpgradeSpec class. */
    public ManagedClusterUpgradeSpec() {
    }

    /**
     * Get the type property: The upgrade type. Full requires the KubernetesVersion property to be set. NodeImageOnly
     * requires the KubernetesVersion property not to be set.
     *
     * @return the type value.
     */
    public ManagedClusterUpgradeType type() {
        return this.type;
    }

    /**
     * Set the type property: The upgrade type. Full requires the KubernetesVersion property to be set. NodeImageOnly
     * requires the KubernetesVersion property not to be set.
     *
     * @param type the type value to set.
     * @return the ManagedClusterUpgradeSpec object itself.
     */
    public ManagedClusterUpgradeSpec withType(ManagedClusterUpgradeType type) {
        this.type = type;
        return this;
    }

    /**
     * Get the kubernetesVersion property: The Kubernetes version to upgrade the member clusters to.
     *
     * @return the kubernetesVersion value.
     */
    public String kubernetesVersion() {
        return this.kubernetesVersion;
    }

    /**
     * Set the kubernetesVersion property: The Kubernetes version to upgrade the member clusters to.
     *
     * @param kubernetesVersion the kubernetesVersion value to set.
     * @return the ManagedClusterUpgradeSpec object itself.
     */
    public ManagedClusterUpgradeSpec withKubernetesVersion(String kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
        return this;
    }

    /**
     * Validates the instance.
     *
     * @throws IllegalArgumentException thrown if the instance is not valid.
     */
    public void validate() {
        if (type() == null) {
            throw LOGGER
                .logExceptionAsError(
                    new IllegalArgumentException("Missing required property type in model ManagedClusterUpgradeSpec"));
        }
    }

    private static final ClientLogger LOGGER = new ClientLogger(ManagedClusterUpgradeSpec.class);
}
