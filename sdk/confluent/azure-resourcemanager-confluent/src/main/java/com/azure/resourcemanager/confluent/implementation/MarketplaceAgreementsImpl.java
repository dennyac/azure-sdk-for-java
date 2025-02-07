// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.confluent.implementation;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.core.http.rest.SimpleResponse;
import com.azure.core.util.Context;
import com.azure.core.util.logging.ClientLogger;
import com.azure.resourcemanager.confluent.fluent.MarketplaceAgreementsClient;
import com.azure.resourcemanager.confluent.fluent.models.ConfluentAgreementResourceInner;
import com.azure.resourcemanager.confluent.models.ConfluentAgreementResource;
import com.azure.resourcemanager.confluent.models.MarketplaceAgreements;

public final class MarketplaceAgreementsImpl implements MarketplaceAgreements {
    private static final ClientLogger LOGGER = new ClientLogger(MarketplaceAgreementsImpl.class);

    private final MarketplaceAgreementsClient innerClient;

    private final com.azure.resourcemanager.confluent.ConfluentManager serviceManager;

    public MarketplaceAgreementsImpl(MarketplaceAgreementsClient innerClient,
        com.azure.resourcemanager.confluent.ConfluentManager serviceManager) {
        this.innerClient = innerClient;
        this.serviceManager = serviceManager;
    }

    public PagedIterable<ConfluentAgreementResource> list() {
        PagedIterable<ConfluentAgreementResourceInner> inner = this.serviceClient().list();
        return Utils.mapPage(inner, inner1 -> new ConfluentAgreementResourceImpl(inner1, this.manager()));
    }

    public PagedIterable<ConfluentAgreementResource> list(Context context) {
        PagedIterable<ConfluentAgreementResourceInner> inner = this.serviceClient().list(context);
        return Utils.mapPage(inner, inner1 -> new ConfluentAgreementResourceImpl(inner1, this.manager()));
    }

    public Response<ConfluentAgreementResource> createWithResponse(ConfluentAgreementResourceInner body,
        Context context) {
        Response<ConfluentAgreementResourceInner> inner = this.serviceClient().createWithResponse(body, context);
        if (inner != null) {
            return new SimpleResponse<>(inner.getRequest(), inner.getStatusCode(), inner.getHeaders(),
                new ConfluentAgreementResourceImpl(inner.getValue(), this.manager()));
        } else {
            return null;
        }
    }

    public ConfluentAgreementResource create() {
        ConfluentAgreementResourceInner inner = this.serviceClient().create();
        if (inner != null) {
            return new ConfluentAgreementResourceImpl(inner, this.manager());
        } else {
            return null;
        }
    }

    private MarketplaceAgreementsClient serviceClient() {
        return this.innerClient;
    }

    private com.azure.resourcemanager.confluent.ConfluentManager manager() {
        return this.serviceManager;
    }
}
