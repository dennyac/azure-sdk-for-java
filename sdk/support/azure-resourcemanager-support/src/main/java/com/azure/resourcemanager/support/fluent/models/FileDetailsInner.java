// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.support.fluent.models;

import com.azure.core.annotation.Fluent;
import com.azure.core.management.ProxyResource;
import com.azure.core.management.SystemData;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;

/** Object that represents File Details resource. */
@Fluent
public final class FileDetailsInner extends ProxyResource {
    /*
     * Properties of the resource
     */
    @JsonProperty(value = "properties")
    private FileDetailsProperties innerProperties;

    /*
     * Azure Resource Manager metadata containing createdBy and modifiedBy information.
     */
    @JsonProperty(value = "systemData", access = JsonProperty.Access.WRITE_ONLY)
    private SystemData systemData;

    /** Creates an instance of FileDetailsInner class. */
    public FileDetailsInner() {
    }

    /**
     * Get the innerProperties property: Properties of the resource.
     *
     * @return the innerProperties value.
     */
    private FileDetailsProperties innerProperties() {
        return this.innerProperties;
    }

    /**
     * Get the systemData property: Azure Resource Manager metadata containing createdBy and modifiedBy information.
     *
     * @return the systemData value.
     */
    public SystemData systemData() {
        return this.systemData;
    }

    /**
     * Get the createdOn property: Time in UTC (ISO 8601 format) when file workspace was created.
     *
     * @return the createdOn value.
     */
    public OffsetDateTime createdOn() {
        return this.innerProperties() == null ? null : this.innerProperties().createdOn();
    }

    /**
     * Get the chunkSize property: Size of each chunk.
     *
     * @return the chunkSize value.
     */
    public Float chunkSize() {
        return this.innerProperties() == null ? null : this.innerProperties().chunkSize();
    }

    /**
     * Set the chunkSize property: Size of each chunk.
     *
     * @param chunkSize the chunkSize value to set.
     * @return the FileDetailsInner object itself.
     */
    public FileDetailsInner withChunkSize(Float chunkSize) {
        if (this.innerProperties() == null) {
            this.innerProperties = new FileDetailsProperties();
        }
        this.innerProperties().withChunkSize(chunkSize);
        return this;
    }

    /**
     * Get the fileSize property: Size of the file to be uploaded.
     *
     * @return the fileSize value.
     */
    public Float fileSize() {
        return this.innerProperties() == null ? null : this.innerProperties().fileSize();
    }

    /**
     * Set the fileSize property: Size of the file to be uploaded.
     *
     * @param fileSize the fileSize value to set.
     * @return the FileDetailsInner object itself.
     */
    public FileDetailsInner withFileSize(Float fileSize) {
        if (this.innerProperties() == null) {
            this.innerProperties = new FileDetailsProperties();
        }
        this.innerProperties().withFileSize(fileSize);
        return this;
    }

    /**
     * Get the numberOfChunks property: Number of chunks to be uploaded.
     *
     * @return the numberOfChunks value.
     */
    public Float numberOfChunks() {
        return this.innerProperties() == null ? null : this.innerProperties().numberOfChunks();
    }

    /**
     * Set the numberOfChunks property: Number of chunks to be uploaded.
     *
     * @param numberOfChunks the numberOfChunks value to set.
     * @return the FileDetailsInner object itself.
     */
    public FileDetailsInner withNumberOfChunks(Float numberOfChunks) {
        if (this.innerProperties() == null) {
            this.innerProperties = new FileDetailsProperties();
        }
        this.innerProperties().withNumberOfChunks(numberOfChunks);
        return this;
    }

    /**
     * Validates the instance.
     *
     * @throws IllegalArgumentException thrown if the instance is not valid.
     */
    public void validate() {
        if (innerProperties() != null) {
            innerProperties().validate();
        }
    }
}
