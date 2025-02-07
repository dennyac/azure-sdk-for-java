// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) TypeSpec Code Generator.
package com.azure.ai.openai.assistants.implementation.models;

import com.azure.ai.openai.assistants.models.FileDetails;
import com.azure.ai.openai.assistants.models.FilePurpose;
import com.azure.core.annotation.Fluent;
import com.azure.core.annotation.Generated;

/**
 * The UploadFileRequest model.
 */
@Fluent
public final class UploadFileRequest {

    /*
     * The file data (not filename) to upload.
     */
    @Generated
    private FileDetails file;

    /*
     * The intended purpose of the file.
     */
    @Generated
    private FilePurpose purpose;

    /*
     * A filename to associate with the uploaded data.
     */
    @Generated
    private String filename;

    /**
     * Creates an instance of UploadFileRequest class.
     *
     * @param file the file value to set.
     * @param purpose the purpose value to set.
     */
    @Generated
    public UploadFileRequest(FileDetails file, FilePurpose purpose) {
        this.file = file;
        this.purpose = purpose;
    }

    /**
     * Get the file property: The file data (not filename) to upload.
     *
     * @return the file value.
     */
    @Generated
    public FileDetails getFile() {
        return this.file;
    }

    /**
     * Get the purpose property: The intended purpose of the file.
     *
     * @return the purpose value.
     */
    @Generated
    public FilePurpose getPurpose() {
        return this.purpose;
    }

    /**
     * Get the filename property: A filename to associate with the uploaded data.
     *
     * @return the filename value.
     */
    @Generated
    public String getFilename() {
        return this.filename;
    }

    /**
     * Set the filename property: A filename to associate with the uploaded data.
     *
     * @param filename the filename value to set.
     * @return the UploadFileRequest object itself.
     */
    @Generated
    public UploadFileRequest setFilename(String filename) {
        this.filename = filename;
        return this;
    }
}
